require 'spec_helper'

describe 'Concurrent writes' do
  it 'performs data corrunption' do
    start_server 1111
    start_server 2222

    client = Client.new 1111
    second_client = Client.new 2222
    client.cmd 'ADD_NODE localhost 2222'
    
    client.cmd 'XPUT key 4'
    second_client.cmd 'PUT key 5'

    client.cmd('GET key').should eq second_client.cmd('GET key')
  end
end