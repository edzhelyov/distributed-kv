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

Thread.abort_on_exception = true

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


          if ['PUT', 'DEL'].include? command
            @nodes.values.each do |node|
              if command == 'PUT'
                node.puts "SYS_PUT #{key} #{value}"
              elsif command == 'DEL'
                node.puts "SYS_DEL #{key} #{value}"
              end
            end
          end

          client.puts response
        end
      end
    end
  end

  def process(command, key, value)
    case command
    when 'PUT', 'SYS_PUT'
      @store[key] = value
      'ok'
    when 'GET'
      @store.fetch(key, :empty)
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

        @store.each do |k, v|
          node.puts "SYS_PUT #{k} #{v}"
        end
        @logger.info "Node #{node_name} database synched"
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
    end
  end

  def stop
    @server.close
  end
end
