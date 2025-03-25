import http from 'node:http';
import process from 'node:process';
import url from 'node:url';

const PORT = parseInt(checkExists(process.env.PORT, 'Must specify PORT env var'), 10);

http.createServer((request, response) => {
  const uri = url.parse(request.url).pathname;
  response.writeHead(200, {'Content-Type': 'text/plain'});

  for (const [key, value] of Object.entries(process.env)) {
    response.write(`${key}: ${value}\n`);
  }
  response.write('\n');

  for (const arg of process.argv) {
    response.write(arg + '\n');
  }
  response.write('\n');

  response.end();
}).listen(PORT);

console.log('Running on port ' + PORT);

function checkExists(v, message = undefined) {
  if (!v) {
    throw new Error(message ?? 'Argument is ' + v);
  }
  return v;
}
