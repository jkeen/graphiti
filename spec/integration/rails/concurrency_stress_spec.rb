if ENV["APPRAISAL_INITIALIZED"]
  require "rails_spec_helper"

  module ConcurrencyStress
    DB_POOL_SIZE = Integer(ENV.fetch("DB_POOL_SIZE", 5))
    POOL_THREADS = 4
    # Its own file-backed database because the suite's shared connection is
    # sqlite :memory:, which gives every pooled connection a separate empty
    # database and so cannot be read from more than one thread.
    DB_PATH = File.expand_path("../../tmp/concurrency_stress.sqlite3", __dir__)
    # Stands in for Postgres round-trip latency: without it every sideload task
    # finishes in microseconds and the queue never reaches its sixteen slots.
    SIDELOAD_DELAY = Float(ENV.fetch("SIDELOAD_DELAY", 0.02))
  end

  RSpec.describe "concurrent sideloading against ActiveRecord" do
    include ConcurrencyHarness

    before(:all) do
      FileUtils.mkdir_p(File.dirname(ConcurrencyStress::DB_PATH))
      FileUtils.rm_f(ConcurrencyStress::DB_PATH)

      stress_record = Class.new(ActiveRecord::Base) do
        self.abstract_class = true
      end
      Object.const_set(:StressRecord, stress_record)
      StressRecord.establish_connection(
        adapter: "sqlite3", database: ConcurrencyStress::DB_PATH, pool: ConcurrencyStress::DB_POOL_SIZE, timeout: 5000
      )

      StressRecord.connection.create_table(:stress_parents, force: true) do |t|
        t.string :name
        t.integer :rank
      end
      StressRecord.connection.create_table(:stress_children, force: true) do |t|
        t.integer :stress_parent_id
        t.string :name
      end

      Object.const_set(:StressParent, Class.new(StressRecord) {
        self.table_name = "stress_parents"
        has_many :stress_children, foreign_key: :stress_parent_id
      })
      Object.const_set(:StressChild, Class.new(StressRecord) {
        self.table_name = "stress_children"
        belongs_to :stress_parent
      })

      Object.const_set(:StressChildResource, Class.new(Graphiti::Resource) {
        self.adapter = Graphiti::Adapters::ActiveRecord
        self.model = StressChild
        self.type = :stress_children
        attribute :stress_parent_id, :integer, only: [:filterable]
        attribute :name, :string
      })

      250.times do |index|
        parent = StressParent.create!(name: "parent-#{index}", rank: index)
        3.times { |child| StressChild.create!(stress_parent_id: parent.id, name: "child-#{child}") }
      end
    end

    after(:all) do
      # Only this class's pool, because the handler is shared with the suite.
      StressRecord.remove_connection
      FileUtils.rm_f(ConcurrencyStress::DB_PATH)
      %i[StressParentResource StressChildResource StressChild StressParent StressRecord].each do |name|
        Object.send(:remove_const, name) if Object.const_defined?(name)
      end
    end

    before do
      allow(Graphiti.config).to receive(:concurrency).and_return(true)
      with_thread_pool(max_threads: ConcurrencyStress::POOL_THREADS)
    end

    # The shape of the airing show page.
    let(:wide_resource) do
      Class.new(Graphiti::Resource) do
        self.adapter = Graphiti::Adapters::ActiveRecord
        self.model = StressParent
        self.type = :stress_parents
        attribute :name, :string
        attribute :rank, :integer

        has_many :intersecting, resource: StressChildResource, foreign_key: :stress_parent_id do
          assign do |parents, _children|
            sleep ConcurrencyStress::SIDELOAD_DELAY
            parents.each { |parent| parent.stress_children.to_a }
          end
        end

        10.times do |index|
          has_many :"branch_#{index}", resource: StressChildResource, foreign_key: :stress_parent_id do
            assign do |_parents, _children|
              sleep ConcurrencyStress::SIDELOAD_DELAY
            end
          end
        end
      end
    end

    let(:wide_include) do
      (["intersecting"] + 10.times.map { |index| "branch_#{index}" }).join(",")
    end

    it "resolves a wide index followed by a wide find, concurrently" do
      resource_class = wide_resource

      run_concurrent_requests(count: ConcurrencyStress::POOL_THREADS * 2, timeout: 90) do
        resource_class.all(page: {size: 250}, fields: {stress_parents: "name,rank"}, sort: "rank").to_a
        resource_class.all(page: {size: 1}, include: wide_include).to_a
      end
    end

    it "leaves the request thread able to query after the pool overflows" do
      resource_class = wide_resource

      run_concurrent_requests(count: ConcurrencyStress::POOL_THREADS * 3, timeout: 90) do
        resource_class.all(page: {size: 1}, include: wide_include).to_a
      end

      expect(StressParent.count).to eq(250)
    end
  end
end
