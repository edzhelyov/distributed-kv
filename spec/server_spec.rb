require 'spec_helper'

describe Server do
  def start_server(pids, port)
    pids << fork do
      trap('TERM') { exit }

      server = Server.new 'localhost', port, Logger.new(nil)
      server.start
    end
    sleep 0.3
  end

  before do
    @pids = []
    start_server @pids, 9876
  end

  after do
    @pids.each do |pid|
      Process.kill :TERM, pid
    end
    sleep 0.3
    @pids.clear
  end

  it 'PUT and GET works' do
    client = Client.new 

    client.cmd 'PUT key 1' 

    client.cmd('GET key').should eq '1'
  end

  it 'LIST works' do
    client = Client.new 

    client.cmd 'PUT key 1' 
    client.cmd 'PUT val 2' 

    client.cmd('LIST').should eq ['2', 'key', 'val']
  end

  it 'DEL works' do
    client = Client.new 

    client.cmd 'PUT key 1' 
    client.cmd 'PUT val 1' 
    client.cmd 'DEL key'

    client.cmd('GET key').should eq 'empty'
  end

  it 'adds a new node' do
  end

  class Client
    def initialize
      @client = TCPSocket.new 'localhost', 9876
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
end
