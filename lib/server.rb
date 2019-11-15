require 'socket'
require 'logger'
require 'concurrent-ruby'
require 'irb'

# Message text until new line
# PUT key value
# GET key
# DEL key
# LIST
# QUIT
#
# ADD_NODE ip port
# REMOVE_NODE ip port
#
# SYS_ADD_NODE
# SYS_REMOVE_NODE ip port
# XPUT sleep 5 seconds afte commit, for testing purposes

Thread.abort_on_exception = true

class Value < Struct.new(:val, :time)
end

class Server
  def initialize(address, port, logger)
    @store = Concurrent::Hash.new
    @nodes = Concurrent::Hash.new
    @logger = logger
    @port = port.to_i
  end

  def start
    @server = TCPServer.open @port
    @logger.info "Started server on port: #{@port}"

    loop do                           
      Thread.new @server.accept do |client|
        _, peer_port, _, peer_ip = client.peeraddr
        client_info = "#{peer_ip}:#{peer_port}"
        @logger.info "Accepted connection from #{client_info}"

        loop do
          begin
            command, key, value = client.gets.split ' '
            command.upcase!
            @logger.info "#{client_info} send #{command}"

            if command == 'QUIT'
              client.close                
              @logger.info "#{client_info} disconnected"
              break
            end

            response = process(command, key, value)

          rescue => e
            response = "error #{e}"
            @logger.error "#{client_info} #{command} Exception: #{e}"
          end


          replicate_command_to_nodes(command, key, value)

          client.puts response
        end
      end
    end
  end

  def process(command, key, value)
    case command
    when 'PUT', 'SYS_PUT'
      store_value(key, value)
      'ok'
    when 'GET'
      val = @store[key]
      if val
        val.val
      else
        :empty
      end
    when 'DEL', 'SYS_DEL'
      @store.delete key
      'ok'
    when 'LIST'
      keys = @store.keys
      resp = [keys.size]
      resp += keys
      resp.join "\n"
    when 'ADD_NODE'
      # TODO: Fix naming key value
      node_name = "#{key}:#{value}"
      if @nodes[node_name]
        'error: node already present'
        @logger.info "Node #{node_name} already present"
      else
        node = TCPSocket.new key, value
        @nodes[node_name] = node
        @logger.info "Node #{node_name} connected"

        node.puts "SYS_ADD_SELF localhost #{@port}"

        sync_data_to_node(node)
        'ok'
      end
    when 'REMOVE_NODE'
      'no op'
    when 'SYS_ADD_SELF'
      node_name = "#{key}:#{value}"
      if @nodes[node_name]
        'error: node already present'
        @logger.info "Node #{node_name} already present"
      else
        node = TCPSocket.new key, value
        @nodes[node_name] = node
        @logger.info "Node #{node_name} added as parent"
      end
    when 'XPUT'
      store_value(key, value)
      @delay_sync = 1
      'ok'
    end
  end

  def store_value(key, value)
    @store[key] = Value.new value, Time.now.utc
  end

  def sync_data_to_node(node)
    @store.each do |k, v|
      node.puts "SYS_PUT #{k} #{v.val}"
    end
    @logger.info "Node #{node_name} database synched"
  end

  def replicate_command_to_nodes(command, key, value)
    if ['PUT', 'XPUT', 'DEL'].include? command
      if @delay_sync
        sleep @delay_sync
        @delay_sync = nil
      end
      @nodes.values.each do |node|
        if command == 'PUT' || command == 'XPUT'
          node.puts "SYS_PUT #{key} #{value}"
        elsif command == 'DEL'
          node.puts "SYS_DEL #{key} #{value}"
        end
      end
    end
  end

  def stop
    @server.close
  end
end
