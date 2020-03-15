require "io/hexdump"
require "amqp-client"

class RMQRecover
  VERSION = "1.0.1"

  record Message,
    exchange : String,
    routing_key : String,
    properties : AMQP::Client::Properties,
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
    {% if flag?(:release) %}
      abort ex.message
    {% else %}
      raise ex
    {% end %}
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
    {% if flag?(:release) %}
      abort ex.message
    {% else %}
      raise ex
    {% end %}
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
        {% unless flag?(:release) %}
        STDERR.puts file
        f = IO::Hexdump.new(f, read: true)
        {% end %}
        extract f, File.extname(file), &blk
      end
    end
  end

  # Yields messages from a rabbitmq message file
  # currently only reads exchange, routing key and body
  # erlang binary format description:
  # http://erlang.org/doc/apps/erts/erl_ext_dist.html
  private def extract(io, ext, &blk : Message -> Nil)
    body = IO::Memory.new
    loop do
      case ext
      when ".idx"
        io.skip 37 # in idx files
      when ".rdq"
        io.read_bytes Int32, IO::ByteFormat::NetworkEndian # 0x00000000
        size = io.read_bytes Int32, IO::ByteFormat::NetworkEndian
        io.skip 18 # ?
      end

      value(io) # basic_message

      io.read_byte # 0x68 small tuple
      io.read_byte # 0x04 small tuple items

      value(io) # "resource"
      vhost = value(io)
      value(io) # "exchange"
      exchange = value(io).as(String)

      io.read_byte # 0x6c (list)
      _ = io.read_bytes Int32, IO::ByteFormat::NetworkEndian # 0001 list items

      rk = value(io).as(String)
      value(io) # nil
      value(io) # "content"
      value(io) # 0x3c (60) some small int?

      io.read_byte # 0x68 small tuple
      io.read_byte # 0x0f small tuple items

      value(io) # "P_basic"

      p = AMQP::Client::Properties.new

      p.content_type = value(io).as?(String)
      p.content_encoding = value(io).as?(String)

      # headers
      case io.read_byte
      when 0x6a # empty table
        io.skip 2 # 0x61 0x02
      when 0x6c # table
        headers_len = io.read_bytes Int32, IO::ByteFormat::NetworkEndian

        headers = AMQ::Protocol::Table.new
        headers_len.times do
          io.read_byte # 0x68 small tuple
          io.read_byte # 0x03 tuple items

          key = value(io).as(String)
          value_type = value(io).as(String)

          value =
            case value_type
            when "bool"
              io.read_string(io.read_bytes Int16, IO::ByteFormat::NetworkEndian) == "true"
            when "long"
              io.skip 2
              io.read_bytes Int16, IO::ByteFormat::NetworkEndian
            when "longstr"
              io.read_string(io.read_bytes Int32, IO::ByteFormat::NetworkEndian)
            end
        end
        p.headers = headers
      end

      if priority = value(io).as?(Int)
        p.priority = priority > UInt8::MAX ? UInt8::MAX : priority.to_u8
      end

      p.correlation_id = value(io).as?(String)
      p.reply_to = value(io).as?(String)
      p.expiration = value(io).as?(String)
      p.message_id = value(io).as?(String)
      if timestamp = value(io).as?(Int)
        p.timestamp = Time.unix timestamp
      end
      p.type = value(io).as?(String)
      p.user_id = value(io).as?(String)
      p.app_id = value(io).as?(String)
      value(io) # reserved1/cluster_id
      value(io) # none?
      value(io) # none?

      io.read_byte # 0x6c list
      io.skip 4 # 0x00000001 list items
      io.read_byte # 0x6d long string
      body_len = io.read_bytes Int32, IO::ByteFormat::NetworkEndian
      IO.copy io, body, body_len
      body.rewind
      io.read_byte # 0x6a nil list tail

      value(io) # garbage?
      value(io) # true

      case ext
      when ".rdq"
        io.read_byte # 0xff
      end

      yield Message.new(exchange, rk, p, body)
    rescue IO::EOFError
      break
    ensure
      body.clear
    end
  end

  private def value(io)
    type = io.read_byte
    case type
    when 0x61
      io.read_byte # uint8
    when 0x62 # int32
     io.read_bytes Int32, IO::ByteFormat::NetworkEndian
    when 0x64 # short string
      v = io.read_string(io.read_bytes Int16, IO::ByteFormat::NetworkEndian)
      v == "undefined" ? nil : v
    when 0x6a # nil
      io.skip 2 # 0x61 0x02
      nil
    when 0x6d # long string
      io.read_string(io.read_bytes Int32, IO::ByteFormat::NetworkEndian)
    else raise "Unknown data type #{type}"
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

USAGE = "Usage: #{PROGRAM_NAME} DIRECTORY [ report | republish URI ]"

path = ARGV.shift? || abort USAGE
mode = ARGV.shift? || abort USAGE

r = RMQRecover.new(path)
case mode
when "report"
  r.report
when "republish"
  uri = ARGV.shift? || abort USAGE
  r.republish uri
else
  abort USAGE
end
