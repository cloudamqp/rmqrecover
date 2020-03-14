require "io/hexdump"
require "amqp-client"

class RMQRecover
  VERSION = "1.0.0"

  record Message,
    exchange : String,
    routing_key : String,
    body : IO::Memory

  EXCHANGE = "exchange"
  MESSAGE = UInt8.static_array(0x6c, 0x00, 0x00, 0x00, 0x01, 0x6d)

  def initialize(@root : String)
  end

  def republish(uri_str)
    uri = URI.parse(uri_str)
    props = AMQP::Client::Properties.new delivery_mode: 2_u8
    vhosts(@root) do |vhost, vhost_path|
      i = 0
      begin
        uri.path = "/#{URI.encode_www_form(vhost)}"
        AMQP::Client.start(uri) do |amqp|
          ch = amqp.channel
          messages(vhost_path) do |msg|
            ch.basic_publish msg.body, msg.exchange, msg.routing_key, props: props
            i += 1
          end
        end
      ensure
        puts "Republished #{i} messages to vhost #{vhost}"
      end
    end
  rescue ex
    abort ex.message
  end

  def report
    vhosts(@root) do |vhost, vhost_path|
      i = 0
      messages(vhost_path) do |msg|
        i += 1
      end
      puts "Found #{i} messages in vhost #{vhost}"
    end
  rescue ex
    abort ex.message
  end

  # finds vhost directories in a directy
  # yields the name of the vhost and the path to it
  private def vhosts(path, &blk : String, String -> Nil)
    Dir.each_child(path) do |c|
      f = File.join path, c
      if c == ".vhost"
        yield File.read(f), path
      elsif File.directory? f
        vhosts(f, &blk)
      end
    end
  end

  # recursivly find files with extension 'idx' or 'rdq'
  # yields matching paths
  private def message_files(path, &blk : String -> Nil)
    Dir.each_child(path) do |c|
      f = File.join(path, c)
      if c.ends_with?(".idx") || c.ends_with?(".rdq")
        yield f
      elsif File.directory? f
        message_files f, &blk
      end
    end
  end

  # recurisvly extract all messages from a directory
  private def messages(path, &blk : Message -> Nil)
    message_files(path) do |file|
      File.open(file) do |f|
        f.buffer_size = 1024 * 1024
        extract f, &blk
      end
    end
  end

  # Yields messages from a rabbitmq message file
  # currently only reads exchange, routing key and body
  # TODO: also parse properties (including headers)
  private def extract(io, &blk : Message -> Nil)
    body = IO::Memory.new
    loop do
      skip_until(io, EXCHANGE)
      io.skip 1
      exchange_len = io.read_bytes Int32, IO::ByteFormat::NetworkEndian
      exchange = io.read_string exchange_len
      io.skip 6
      rk_len = io.read_bytes Int32, IO::ByteFormat::NetworkEndian
      rk = io.read_string rk_len
      skip_until(io, MESSAGE)
      body_len = io.read_bytes Int32, IO::ByteFormat::NetworkEndian
      IO.copy io, body, body_len
      body.rewind
      yield Message.new(exchange, rk, body)
    rescue IO::EOFError
      break
    ensure
      body.clear
    end
  end

  # searches in a byte stream for a match, one byte at a time
  private def skip_until(io, slice) : Nil
    match = slice.to_slice
    window = Bytes.new(match.size)
    until window == match
      window.map_with_index! do |v, i|
        if i < window.size - 1
          window[i + 1]
        else
          io.read_byte || raise IO::EOFError.new
        end
      end
    end
  end
end

usage = "Usage: #{PROGRAM_NAME} DIRECTORY [ report | republish URI ]"

path = ARGV.shift? || abort usage
mode = ARGV.shift? || abort usage

r = RMQRecover.new(path)
case mode
when "report"
  r.report
when "republish"
  uri = ARGV.shift? || abort usage
  r.republish uri
else
  abort usage
end
