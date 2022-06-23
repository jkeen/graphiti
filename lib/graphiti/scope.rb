module Graphiti
  class Scope
    attr_accessor :object, :unpaginated_object
    attr_reader :pagination
    def initialize(object, resource, query, opts = {})
      @object = object
      @resource = resource
      @query = query
      @opts = opts

      @object = @resource.around_scoping(@object, @query.hash) { |scope|
        apply_scoping(scope, opts)
      }
    end

    def resolve
      if @query.zero_results?
        []
      else
        resolved = broadcast_data { |payload|
          @object = @resource.before_resolve(@object, @query)
          payload[:results] = @resource.resolve(@object)
          payload[:results]
        }
        resolved.compact!
        assign_serializer(resolved)
        yield resolved if block_given?
        @opts[:after_resolve]&.call(resolved)
        resolve_sideloads(resolved) unless @query.sideloads.empty?
        resolved
      end
    end

    def resolve_sideloads(results)
      return if results == []

      concurrent = Graphiti.config.concurrency
      promises = []

      @query.sideloads.each_pair do |name, q|
        sideload = @resource.class.sideload(name)
        next if sideload.nil? || sideload.shared_remote?
        parent_resource = @resource
        graphiti_context = Graphiti.context
        resolve_sideload = -> {
          Graphiti.context = graphiti_context
          sideload.resolve(results, q, parent_resource)
          @resource.adapter.close if concurrent
        }
        if concurrent
          promises << Concurrent::Promise.execute(&resolve_sideload)
        else
          resolve_sideload.call
        end
      end

      if concurrent
        # Wait for all promises to finish
        sleep 0.01 until promises.all? { |p| p.fulfilled? || p.rejected? }
        # Re-raise the error with correct stacktrace
        # OPTION** to avoid failing here?? if so need serializable patch
        # to avoid loading data when association not loaded
        if (rejected = promises.find(&:rejected?))
          raise rejected.reason
        end
      end
    end

    def parent_resource
      @resource
    end

    def cache_key
      query_cache_key
    end

    def updated_at
      last_modified_at
    end

    private

    def query_cache_key
      # This is the combined cache key for the base query and the query for all sideloads
      # If any returned model's updated_at changes, this key will change

      @object = @resource.before_resolve(@object, @query)
      results = @resource.resolve(@object)

      cache_keys = []
      unless @query.sideloads.empty?
        @query.sideloads.each_pair do |name, q|
          sideload = @resource.class.sideload(name)
          next if sideload.nil? || sideload.shared_remote?

          cache_keys << sideload.build_resource_proxy(results, q, parent_resource).cache_key
        end
      end

      cache_keys << @object.cache_key_with_version # this is what calls into ActiveRecord
      cache_keys.flatten.join('+')
    end

    def last_modified_at
      @object = @resource.before_resolve(@object, @query)
      results = @resource.resolve(@object)
      updated_ats = []
      unless @query.sideloads.empty?
        @query.sideloads.each_pair do |name, q|

          puts "SIDELOAD: #{name}"
          sideload = @resource.class.sideload(name)
          next if sideload.nil? || sideload.shared_remote?

          updated_ats << sideload.build_resource_proxy(results, q, parent_resource).updated_at
        end
      end

      updated_at = @object.maximum(:updated_at) # this is what makes the query
      updated_ats.concat([updated_at]).max
    end

    private

    def broadcast_data
      opts = {
        resource: @resource,
        params: @opts[:params],
        sideload: @opts[:sideload],
        parent: @opts[:parent]
        # Set once data is resolved within block
        #   results: ...
      }
      Graphiti.broadcast(:resolve, opts) do |payload|
        yield payload
      end
    end

    # Used to ensure the resource's serializer is used
    # Not one derived through the usual jsonapi-rb logic
    def assign_serializer(records)
      records.each_with_index do |r, index|
        @resource.decorate_record(r, index)
      end
    end

    def apply_scoping(scope, opts)
      @object = scope

      unless @resource.remote?
        opts[:default_paginate] = false unless @query.paginate?
        add_scoping(nil, Graphiti::Scoping::DefaultFilter, opts)
        add_scoping(:filter, Graphiti::Scoping::Filter, opts)
        add_scoping(:sort, Graphiti::Scoping::Sort, opts)
        add_scoping(:paginate, Graphiti::Scoping::Paginate, opts)
      end

      @object
    end

    def add_scoping(key, scoping_class, opts, default = {})
      @object = scoping_class.new(@resource, @query.hash, @object, opts).apply
      @unpaginated_object = @object unless key == :paginate
    end
  end
end
