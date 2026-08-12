require "spec_helper"
require "yaml"

# Allocation counts rather than wall time: they are deterministic under CRuby,
# so a budget catches per-record work that a timing assertion could only find
# by being flaky.
#
# Baselines live in baselines.yml and are compared rather than hardcoded, so
# that file's git history is the record of how each release performed.
# Regenerate with:
#
#   PERF_BASELINE=write bundle exec rspec spec/performance
#
# Rewriting it is deliberate: a diff that raises a number needs the same
# justification in review as any other change, and one that lowers a number
# ratchets the ceiling down so a gain cannot quietly be given back.
module AllocationBaselines
  PATH = File.expand_path("baselines.yml", __dir__)
  TOLERANCE = 0.03

  # Both phases, because resolving and rendering regress for different reasons
  # and a combined number cannot tell them apart.
  PHASES = {
    "resolve" => ->(proxy) { proxy.to_a },
    "render" => ->(proxy) { proxy.to_jsonapi }
  }.freeze

  # Concurrency is what production runs, and its cost lives on a separate code
  # path: graphiti-api/graphiti#510 lowered the synchronous path and left this
  # one untouched, which a single-mode budget would never show.
  MODES = ["off", "on"].freeze

  SCENARIOS = {
    "flat/10" => {seed: {employees: 10}, params: {page: {size: 10}}},
    "flat/100" => {seed: {employees: 100}, params: {page: {size: 100}}},
    "flat/100 sparse" => {
      seed: {employees: 100},
      params: {page: {size: 100}, fields: {employees: "first_name"}}
    },
    "nested/50 one level" => {
      seed: {employees: 50, positions_per_employee: 4, departments: 5},
      params: {page: {size: 50}, include: "positions"}
    },
    "nested/50 two levels" => {
      seed: {employees: 50, positions_per_employee: 4, departments: 5},
      params: {page: {size: 50}, include: "positions.department"}
    },
    "nested/50 three levels" => {
      seed: {employees: 50, positions_per_employee: 4, departments: 5},
      params: {page: {size: 50}, include: "positions.department.positions"}
    },
    "stats/100" => {
      seed: {employees: 100},
      params: {page: {size: 100}, stats: {total: "count"}}
    }
  }.freeze
end

RSpec.describe "allocation baselines" do
  def seed(employees:, positions_per_employee: 0, departments: 1)
    department_ids = Array.new(departments) { |index| PORO::Department.create(name: "d#{index}").id }

    employees.times do |employee_index|
      employee = PORO::Employee.create(first_name: "f#{employee_index}", age: 30)
      positions_per_employee.times do |position_index|
        PORO::Position.create(
          employee_id: employee.id,
          title: "t#{position_index}",
          department_id: department_ids[position_index % department_ids.length]
        )
      end
    end
  end

  def allocations
    GC.start(full_mark: true, immediate_sweep: true)
    GC.disable
    before = GC.stat[:total_allocated_objects]
    yield
    GC.stat[:total_allocated_objects] - before
  ensure
    GC.enable
  end

  # The first pass loads constants and fills memoized state that later passes
  # reuse, so it is not representative.
  def measure(scenario, phase, mode)
    seed(**scenario[:seed])
    with_concurrency(mode) do
      phase.call(PORO::EmployeeResource.all(scenario[:params]))
      allocations { phase.call(PORO::EmployeeResource.all(scenario[:params])) }
    end
  end

  # Set rather than stubbed: a stub on a path this measures would allocate
  # rspec-mocks bookkeeping and land in the numbers.
  #
  # The pool constant is a delay resolved on first touch, so it has to be
  # replaced rather than reconfigured.
  def with_concurrency(mode)
    concurrent = mode == "on"
    previous = Graphiti.config.concurrency
    Graphiti.config.concurrency = concurrent
    if concurrent
      stub_const(
        "Graphiti::Scope::GLOBAL_THREAD_POOL_EXECUTOR",
        Concurrent::Promises.delay do
          Concurrent::ThreadPoolExecutor.new(
            min_threads: 0, max_threads: 4, max_queue: 16, fallback_policy: :caller_runs
          )
        end
      )
    end
    yield
  ensure
    Graphiti.config.concurrency = previous
  end

  if ENV["PERF_BASELINE"] == "write"
    it "records a baseline for every scenario" do
      recorded = {}
      AllocationBaselines::MODES.each do |mode|
        AllocationBaselines::SCENARIOS.each_pair do |name, scenario|
          AllocationBaselines::PHASES.each_pair do |phase_name, phase|
            recorded["#{name} #{phase_name} concurrency:#{mode}"] = measure(scenario, phase, mode)
            PORO::DB.clear
          end
        end
      end

      File.write(AllocationBaselines::PATH, recorded.to_yaml)
      puts "\nwrote #{AllocationBaselines::PATH}"
      recorded.each { |name, count| puts format("  %-30s %8d", name, count) }
    end
  else
    let(:baselines) { YAML.safe_load_file(AllocationBaselines::PATH) }

    AllocationBaselines::MODES.each do |mode|
      AllocationBaselines::SCENARIOS.each_pair do |name, scenario|
        AllocationBaselines::PHASES.each_pair do |phase_name, phase|
          it "#{name} #{phase_name} holds its baseline with concurrency #{mode}" do
            key = "#{name} #{phase_name} concurrency:#{mode}"
            baseline = baselines.fetch(key) {
              raise "no baseline for #{key.inspect}, regenerate with PERF_BASELINE=write"
            }
            count = measure(scenario, phase, mode)
            drift = (count - baseline) / baseline.to_f

            expect(drift).to be <= AllocationBaselines::TOLERANCE,
              "#{key} allocated #{count} against a baseline of #{baseline} " \
              "(#{(drift * 100).round(1)}% drift)"
          end
        end
      end
    end
  end
end
