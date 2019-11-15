require_relative 'lib/server'

port = (ARGV.first || 2000).to_i
server = Server.new 'localhost', port, Logger.new(STDOUT)

server.start
