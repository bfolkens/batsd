module Batsd
  # 
  # This is a thin wrapper around the redis client to
  # handle multistep procedures that could be executed using
  # Redis scripting
  #
  class Redis

    # Opens a new connection to the redis instance specified 
    # in the configuration or localhost:6379
    #
    def initialize(options)
      @redis = ::Redis.new(options[:redis] || {host: "127.0.0.1", port: 6379} )
      @redis.ping
      @lua_support = @redis.info['redis_version'].to_f >= 2.5
      @retentions = options[:retentions].keys
    end
    
    # Expose the redis client directly
    def client
      @redis
    end

    # Store a value to a zset
    def store_value(timestamp, key, value)
      @redis.zadd key, timestamp, value
    end

    # Store unaggregated, raw timer values in bucketed keys
    # so that they can actually be aggregated "raw"
    def store_for_aggregations(key, values)
      @retentions.each_with_index do |t, index|
        next if index.zero?
        @redis.sadd "acc-#{key}:#{t}", values
        @redis.expire "acc-#{key}:#{t}", t.to_i * 2
      end
    end

    # Pop all the raw members from the set for aggregation
    def pop_for_aggregations(key, retention)
      [].tap do |values|
        while value = @redis.spop("acc-#{key}:#{retention}")
          values << value
        end
      end
    end
    
    # Returns the value of a key and then deletes it.
    def get_and_clear_key(key)
      if @lua_support
        cmd = <<-EOF
          local str = redis.call('get', KEYS[1])
          redis.call('del', KEYS[1])
          return str
        EOF
        @redis.eval(cmd, [key.to_sym])
      else
        @redis.multi do |multi|
          multi.get(key)
          multi.del(key)
        end.first
      end
    end

    # Deletes the given key
    def clear_key(key)
      @redis.del(key)
    end
    
    # Truncate a zset since a treshold time
    #
    def truncate_zset(key, since)
      @redis.zremrangebyscore key, 0, since
    end

    # Return properly formatted values from the zset
    def values_from_zset(metric, begin_ts, end_ts)
      begin
        values = @redis.zrangebyscore(metric, begin_ts, end_ts, with_scores: true)
        values.collect {|val, ts| { timestamp: ts.to_i, value: val.to_f }}
      rescue
        []
      end
    end

    # Convenience accessor to members of datapoints set
    #
    def datapoints(with_gauges=true)
      datapoints = @redis.smembers "datapoints"
      unless with_gauges
        datapoints.reject!{|d| (d.match(/^gauge/) rescue false) }
      end
      datapoints
    end

    # Stores a reference to the datapoint in 
    # the 'datapoints' set
    #
    def add_datapoint(key)
      @redis.sadd "datapoints", key
    end

    # Stores a reference to the datapoint in 
    # the 'datapoints' set
    #
    def remove_datapoint(key)
      @redis.srem "datapoints", key
    end

  end
end
