if ENV["APPRAISAL_INITIALIZED"]
  require "rails_spec_helper"

  # Sibling sideloads run on different threads and write to the same shared
  # record. Nothing in graphiti makes that safe - it holds because of how Ruby
  # schedules threads. This test fails if that ever stops being true.
  RSpec.describe "dedup against ActiveRecord's association cache" do
    include ConcurrencyHarness

    before(:all) do
      @db_path = File.expand_path("../../tmp/dedup_probe.sqlite3", __dir__)
      FileUtils.mkdir_p(File.dirname(@db_path))
      FileUtils.rm_f(@db_path)
      Object.const_set(:DedupRecord, Class.new(ActiveRecord::Base) { self.abstract_class = true })
      DedupRecord.establish_connection(adapter: "sqlite3", database: @db_path, pool: 10, timeout: 5000)

      DedupRecord.connection.create_table(:dedup_parents, force: true) { |t| t.string :name }
      DedupRecord.connection.create_table(:dedup_children, force: true) do |t|
        t.integer :dedup_parent_id
        t.string :kind
      end

      Object.const_set(:DedupParent, Class.new(DedupRecord) {
        self.table_name = "dedup_parents"
        has_many :alphas, -> { where(kind: "alpha") }, class_name: "DedupChild", foreign_key: :dedup_parent_id
        has_many :betas, -> { where(kind: "beta") }, class_name: "DedupChild", foreign_key: :dedup_parent_id
      })
      Object.const_set(:DedupChild, Class.new(DedupRecord) {
        self.table_name = "dedup_children"
        belongs_to :dedup_parent
      })

      Object.const_set(:DedupChildResource, Class.new(Graphiti::Resource) {
        self.adapter = Graphiti::Adapters::ActiveRecord
        self.model = DedupChild
        self.type = :dedup_children
        attribute :dedup_parent_id, :integer, only: [:filterable]
        attribute :kind, :string
      })
      Object.const_set(:DedupParentResource, Class.new(Graphiti::Resource) {
        self.adapter = Graphiti::Adapters::ActiveRecord
        self.model = DedupParent
        self.type = :dedup_parents
        attribute :name, :string
        has_many :alphas, resource: DedupChildResource, foreign_key: :dedup_parent_id
        has_many :betas, resource: DedupChildResource, foreign_key: :dedup_parent_id
      })

      60.times do |i|
        parent = DedupParent.create!(name: "p#{i}")
        2.times { DedupChild.create!(dedup_parent_id: parent.id, kind: "alpha") }
        2.times { DedupChild.create!(dedup_parent_id: parent.id, kind: "beta") }
      end
    end

    after(:all) do
      DedupRecord.remove_connection
      FileUtils.rm_f(@db_path)
      %i[DedupParentResource DedupChildResource DedupChild DedupParent DedupRecord].each do |name|
        Object.send(:remove_const, name) if Object.const_defined?(name)
      end
    end

    before do
      allow(Graphiti.config).to receive(:concurrency).and_return(true)
      with_thread_pool(max_threads: 4)
    end

    it "keeps both sibling associations on every shared instance" do
      damaged = []

      8.times do |run|
        parents = DedupParentResource.all(page: {size: 60}, include: "alphas,betas").to_a
        parents.each do |parent|
          cache = parent.instance_variable_get(:@association_cache) || {}
          loaded = cache.keys.sort
          damaged << [run, parent.id, loaded] unless loaded == [:alphas, :betas]
        end
      end

      expect(damaged).to be_empty, "lost associations on #{damaged.size} parents, e.g. #{damaged.first(3).inspect}"
    end
  end
end
