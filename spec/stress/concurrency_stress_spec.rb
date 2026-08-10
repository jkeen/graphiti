require "spec_helper"

RSpec.describe "concurrent sideloading under load" do
  include ConcurrencyHarness

  let(:pool_size) { 4 }

  before do
    allow(Graphiti.config).to receive(:concurrency).and_return(true)
    with_thread_pool(max_threads: pool_size)
  end

  let(:employee_resource_with_blocking_assign) do
    Class.new(PORO::EmployeeResource) do
      self.model = PORO::Employee

      has_many :positions, resource: PORO::PositionResource do
        assign do |employees, positions|
          PORO::DepartmentResource.all(page: {size: 1}).to_a

          employees.each do |employee|
            employee.positions = positions.select { |position| position.employee_id == employee.id }
          end
        end
      end
    end
  end

  let(:employee_resource_with_four_blocking_assigns) do
    Class.new(PORO::EmployeeResource) do
      self.model = PORO::Employee

      {
        positions: PORO::PositionResource,
        credit_cards: PORO::CreditCardResource,
        visas: PORO::VisaResource,
        gold_visas: PORO::GoldVisaResource
      }.each_pair do |association_name, resource_class|
        has_many association_name, resource: resource_class do
          assign do |_employees, _children|
            PORO::DepartmentResource.all(page: {size: 1}).to_a
          end
        end
      end
    end
  end

  def resolve(resource_class, params)
    resource_class.all({page: {size: 100}}.merge(params)).to_a
  end

  describe "a hook that resolves another resource synchronously" do
    before { seed_employees(count: 20, positions_per_employee: 3, departments: 4) }

    it "does not deadlock when concurrent requests exceed the pool size" do
      resource_class = employee_resource_with_blocking_assign

      run_concurrent_requests(count: pool_size * 2, timeout: 10) do
        resolve(resource_class, include: "positions")
      end
    end

    it "does not deadlock within a single request whose blocking sideloads match the pool size" do
      resource_class = employee_resource_with_four_blocking_assigns

      run_concurrent_requests(count: 1, timeout: 10) do
        resolve(resource_class, include: "positions,credit_cards,visas,gold_visas")
      end
    end

    it "resolves the same graph with concurrency off" do
      allow(Graphiti.config).to receive(:concurrency).and_return(false)
      resource_class = employee_resource_with_four_blocking_assigns

      run_concurrent_requests(count: pool_size * 2, timeout: 10) do
        resolve(resource_class, include: "positions,credit_cards,visas,gold_visas")
      end
    end
  end

  let(:self_referential_resource) do
    Class.new(PORO::EmployeeResource) do
      self.model = PORO::Employee

      has_many :intersecting, resource: PORO::EmployeeResource, foreign_key: :id do
        assign do |employees, _intersections|
          employees.each do |employee|
            PORO::DB.data[:positions].select { |position| position[:employee_id] == employee.id }
          end
        end
      end
    end
  end

  describe "a self-referential sideload whose assign lazy-loads per parent" do
    before { seed_employees(count: 30, positions_per_employee: 4, departments: 5) }

    it "resolves without stalling" do
      resource_class = self_referential_resource

      run_concurrent_requests(count: pool_size * 2, timeout: 20) do
        resolve(resource_class, include: "intersecting.positions.department,positions.department")
      end
    end

    it "keeps each parent's own branches intact" do
      resource_class = self_referential_resource

      results = run_concurrent_requests(count: pool_size * 2, timeout: 20) do
        resolve(resource_class, include: "intersecting.positions,positions.department")
      end

      results.each do |employees|
        expect(employees.length).to eq(30)

        employees.each do |employee|
          expect(employee.positions.length).to eq(4)
          expect(employee.positions.map(&:employee_id).uniq).to eq([employee.id])
        end
      end
    end
  end

  let(:many_sideload_resource) do
    Class.new(PORO::EmployeeResource) do
      self.model = PORO::Employee

      has_many :intersecting, resource: PORO::EmployeeResource, foreign_key: :id do
        assign do |employees, _children|
          employees.each do |employee|
            PORO::DB.data[:positions].select { |position| position[:employee_id] == employee.id }
          end
        end
      end

      10.times do |index|
        has_many :"branch_#{index}", resource: PORO::PositionResource, foreign_key: :employee_id do
          assign { |_parents, _children| }
        end
      end
    end
  end

  describe "a find with more sideloads than the pool has threads" do
    before { seed_employees(count: 400, positions_per_employee: 3, departments: 8) }

    let(:wide_include) do
      (["intersecting", "intersecting.positions"] +
        10.times.flat_map { |index| ["branch_#{index}", "branch_#{index}.department"] }).join(",")
    end

    it "resolves a wide index followed by a wide find, concurrently" do
      resource_class = many_sideload_resource

      run_concurrent_requests(count: pool_size * 2, timeout: 60) do
        resource_class.all(page: {size: 1000}, fields: {employees: "first_name,age"}, sort: "age").to_a

        resource_class.all(
          page: {size: 1},
          include: wide_include
        ).to_a
      end
    end
  end

  describe "deep include trees" do
    before { seed_employees(count: 30, positions_per_employee: 4, departments: 5) }

    it "resolves every branch under concurrent load" do
      results = run_concurrent_requests(count: pool_size * 3, timeout: 30) do
        resolve(PORO::EmployeeResource, include: "positions.department.positions")
      end

      results.each do |employees|
        expect(employees.length).to eq(30)

        employees.each do |employee|
          expect(employee.positions.length).to eq(4)

          employee.positions.each do |position|
            expect(position.department).not_to be_nil
            expect(position.department.positions).not_to be_empty
          end
        end
      end
    end
  end

  describe "retention across rounds" do
    before { seed_employees(count: 25, positions_per_employee: 4, departments: 5) }

    it "does not accumulate live objects" do
      rounds = 10
      warmup_rounds = 2
      samples = []

      rounds.times do
        run_concurrent_requests(count: pool_size, timeout: 30) do
          resolve(PORO::EmployeeResource, include: "positions.department")
        end

        samples << live_slots
      end

      settled = samples[warmup_rounds]
      growth = samples.last - settled

      expect(growth).to be < settled * 0.1,
        "live objects grew #{growth} slots (#{(growth * 100.0 / settled).round(1)}%) " \
        "over #{rounds - warmup_rounds} rounds: #{samples.inspect}"
    end
  end

  describe "debugger under concurrent requests" do
    before do
      seed_employees(count: 10, positions_per_employee: 3, departments: 3)
      @original_logger = Graphiti.logger
      Graphiti.logger = Logger.new(IO::NULL)
      Graphiti::Debugger.enabled = true
    end

    after do
      Graphiti::Debugger.enabled = false
      Graphiti::Debugger.chunks = []
      Graphiti.logger = @original_logger
    end

    def capture_flushes
      flushes = Concurrent::Array.new
      subscriber = ActiveSupport::Notifications.subscribe("flush_debug.graphiti") do |*, payload|
        flushes << {chunk_count: payload[:chunks].size, array_id: payload[:chunks].object_id}
      end
      yield
      flushes.to_a
    ensure
      ActiveSupport::Notifications.unsubscribe(subscriber)
    end

    def debugged_request
      Graphiti::Debugger.debug do
        resolve(PORO::EmployeeResource, include: "positions.department")
      end
    end

    it "flushes each request's own chunks when requests overlap" do
      alone = capture_flushes {
        Graphiti.with_context({}, :index) { debugged_request }
      }.first

      overlapping = capture_flushes {
        run_concurrent_requests(count: pool_size * 2, timeout: 30) { debugged_request }
      }

      expect(overlapping.map { |flush| flush[:array_id] }.uniq.size).to eq(overlapping.size)
      expect(overlapping.map { |flush| flush[:chunk_count] }.uniq).to eq([alone[:chunk_count]])
    end
  end
end
