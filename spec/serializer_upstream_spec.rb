require "spec_helper"

# Graphiti::Serializer#initialize reimplements this one so it can leave
# relationships unbuilt. These pin what that copy has to keep doing.
RSpec.describe JSONAPI::Serializable::Resource do
  let(:upstream) do
    Class.new(JSONAPI::Serializable::Resource) do
      type :widgets
      attribute(:name) { @object.name }
      relationship(:parts) { data { [] } }
    end
  end

  let(:instance) { upstream.new(object: OpenStruct.new(id: 1, name: "a")) }

  it "sets the state the copy replicates" do
    expect(instance.instance_variables.sort)
      .to eq(%i[@_exposures @_id @_meta @_relationships @_type @object])
  end

  it "builds every relationship up front, which is what the copy defers" do
    expect(instance.instance_variable_get(:@_relationships).keys).to eq([:parts])
  end

  it "freezes, which the copy does not, because it stashes the include set" do
    expect(instance).to be_frozen
  end
end
