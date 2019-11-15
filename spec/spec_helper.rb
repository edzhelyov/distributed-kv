require 'server'

RSpec.configure do |config|
  config.expect_with(:rspec) { |c| c.syntax = :should }
  config.before do
    @pids = []
  end
  config.after do
    @pids.each do |pid|
      Process.kill :TERM, pid
    end
    sleep 0.1
    @pids.clear
  end
end

class Client
  def initialize(port = 9876)
    @client = TCPSocket.new 'localhost', port
  end

  def cmd(cmd)
    @client.puts cmd

    if cmd == 'LIST'
      res = [@client.gets.strip]
      res[0].to_i.times do
        res << @client.gets.strip
      end

      res
    else
      @client.gets.strip
    end
  end
end

def start_server(port, params = {})
  @pids << fork do
    trap('TERM') { exit }

    server = Server.new 'localhost', port, Logger.new(nil), offset: params[:offset]
    server.start
  end
  sleep 0.1
end
