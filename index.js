const express = require('express');
const { createServer } = require('http');
const { join } = require('path');
const { Server } = require('socket.io');
const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const { availableParallelism } = require('os');
const cluster = require('cluster');
const { createAdapter, setupPrimary } = require('@socket.io/cluster-adapter');

if (cluster.isPrimary) {
  const numCPUs = availableParallelism();
  // create one worker per available core
  for (let i = 0; i < numCPUs; i++) {
    cluster.fork({
      PORT: 8000 + i
    });
  }

  // set up the adapter on the primary thread
  return setupPrimary();
}

async function main() {
  // open the database file
  const db = await open({
    filename: 'chat.db',
    driver: sqlite3.Database
  });

  // create our message table
  await db.exec(`
    CREATE TABLE IF NOT EXISTS messages(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_offset TEXT UNIQUE,
    content TEXT)
    `);

  const app = express();
  const server = createServer(app);
  const io = new Server(server, {
    connectionStateRecovery: {},
    // set up the adapter on each worker thread
    adapter: createAdapter()
  });

  app.get('/', (req, res, next) => {
    res.sendFile(join(__dirname, "./index.html"));
  });

  io.on('connection', async (socket) => {
    console.log("A user Connected.");

    socket.on('chat-message', async (msg, clientOffset, callback) => {
      // io.emit('chat-message', msg);
      let result;
      try {
        // store the message in the database
        result = await db.run('INSERT INTO messages (content, client_offset) VALUES (?, ?)', msg, clientOffset);
      } catch (error) {
        // handle the failure
        if (error.errno === 19 /* SQLITE_CONSTRAINT */) {
          // the message was already inserted, so we notify the client
          callback();
        } else {
          // nothing to do, just let the client retry
        }
        return;
      }
      // include the offset with the message
      io.emit('chat-message', msg, result.lastID);
      // acknowledge the event
      callback();
    });

    if (!socket.recovered) {
      // if the connection state recovery was not successful
      try {
        await db.each('SELECT id, content FROM messages WHERE id > ?',
          [socket.handshake.auth.serverOffset || 0],
          (_err, row) => {
            socket.emit('chat-message', row.content, row.id);
          }
        )
      } catch (error) {
        console.log(error.message);

      }
    }

    socket.on('disconnect', () => {
      console.log('User disconnected.');
    });
  });

  const port = process.env.PORT;
  server.listen(port, () => {
    console.log(`Server is listening at port ${port}.`);
  });
};

main();