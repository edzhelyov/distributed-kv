require 'spec_helper'

describe Server do
  it 'PUT and GET works' do
    start_server 9876
    client = Client.new 9876

    client.cmd 'PUT key 1' 

    client.cmd('GET key').should eq '1'
  end

  it 'LIST works' do
    start_server 9876
    client = Client.new 9876

    client.cmd 'PUT key 1' 
    client.cmd 'PUT val 2' 

    client.cmd('LIST').should eq ['2', 'key', 'val']
  end

  it 'DEL works' do
    start_server 9876
    client = Client.new 9876

    client.cmd 'PUT key 1' 
    client.cmd 'PUT val 1' 
    client.cmd 'DEL key'

    client.cmd('GET key').should eq 'empty'
  end

  it 'adds a new node' do
    start_server 9876
    start_server 1111
    start_server 2222

    client = Client.new 9876
    second_client = Client.new 1111
    third_client = Client.new 2222

    client.cmd 'PUT key 1'
    client.cmd 'ADD_NODE localhost 1111'
    sleep 0.3
    second_client.cmd('LIST').should eq ['1', 'key']

    second_client.cmd 'ADD_NODE localhost 2222'
    sleep 0.3
    second_client.cmd 'PUT val 2'

    third_client.cmd('LIST').should eq ['2', 'key', 'val']
  end
end
