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
    end
    
    # Expose the redis client directly
    def client
      @redis
    end

    # Store a value to a zset
    def store_value(timestamp, key, value)
      # Prepend the timestamp so we always have unique values
      @redis.zadd(key, timestamp, "#{timestamp}:#{value}")
    end

    # Store unaggregated, raw timer values in bucketed keys
    # so that they can actually be aggregated "raw"
    def store_for_aggregations(retentions, key, values)
      retentions.each_with_index do |t, index|
        next if index.zero?
        @redis.lpush "acc-#{key}:#{t}", values
        @redis.expire "acc-#{key}:#{t}", t.to_i * 2
      end
    end

    # Pop all the raw members from the set for aggregation
    def pop_for_aggregations(key, retention)
      [].tap do |values|
        while value = @redis.rpop("acc-#{key}:#{retention}")
          values << value
        end
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
      values = @redis.zrangebyscore(metric, begin_ts, end_ts, with_scores: true)
      values.collect do |point, ts|
        val = point.split(':').last
        { timestamp: ts.to_i, value: val.to_f }
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
