# encode: utf-8
class Fluent::KafkaETLOutputBuffered < Fluent::BufferedOutput
  Fluent::Plugin.register_output('kafka_etl', self)
  
  def initialize
    super
    require 'poseidon'
    require 'json'
  end
  
  config_param :brokers, :string, :default => 'localhost:9092'
  config_param :topic, :string, :default => 'etl_queue'
  config_param :client_id, :string, :default => "fluentd_#{ENV['HOSTNAME']}"
  config_param :bulk_mode, :bool, :default => false
  
  def configure(conf)
    super
    @seed_brokers = @brokers.match(",").nil? ? [@brokers] : @brokers.split(",")
    
    @partitioner = Proc.new { |key, partition_count|  Zlib::crc32(key) % partition_count }
    
  end

  def start
    super
  end

  def shutdown
    super
  end

  def format(tag, time, record)
    if @bulk_mode
      unless record.end_with?("\n")
        record << "\n"
      else
        record
      end
    else
      "" << tag << "\t" << time.to_i << "\t" << record.to_json << "\n"
    end
  end

  def write(chunk)
    producer = Poseidon::Producer.new(@seed_brokers,
                                      @client_id,
                                      :type => :sync,
                                      :compression_codec => :gzip,
                                      :max_send_retries => 3,
                                      :retry_backoff_ms => 100,
                                      :required_acks => 1,
                                      :ack_timeout_ms => 3000)
    #                                  :partitioner => @partitioner)
    
    start_time = Time.now.to_i
    $log.info("send start time: #{start_time}")
    
    num_send = 0
    messages = []
    chunk.open do |io|
      while record = io.gets
        (tag, timestamp, message) = record.split("\t", 3)
        t = Time.at(timestamp.to_i)
        date = t.strftime("%Y-%m-%d")
        hour = t.strftime("%H")
        messages << Poseidon::MessageToSend.new(@topic, message, "#{tag}/#{date}/#{hour}")
        if messages.size >= 1000
          num_send += messages.size
          producer.send_messages(messages)
          messages = []
        end
      end
    end
    num_send += messages.size
    producer.send_messages(messages) if messages.size > 0
    end_time = Time.now.to_i
    
    send_time = end_time - start_time
    $log.info("send proc time: #{send_time} num: #{num_send}")
    
    producer.shutdown
  end

end
