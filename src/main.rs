use std::io;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use serde_json::Result;

#[derive(Default, Debug)]
struct Node {
    id: String,
    msg_id: u128,
    node_ids: Vec<String>,
    neighbors: Vec<String>,
    messages: HashSet<u128>,
}

impl Node {
    pub fn new() -> Node {
        Default::default()
    }

    fn reply(&mut self, request: Message, mut reply: MessageBody) -> Result<()> {
        reply.in_reply_to = request.body.msg_id;
        self.send(request.src, reply)
    }

    fn send(&mut self, dest: String, mut reply: MessageBody) -> Result<()> {
        self.msg_id += 1;
        reply.msg_id = self.msg_id;

        let out = Message {
            src: self.id.clone(),
            dest: dest,
            body: reply,
        };

        let out_str = serde_json::to_string(&out)?;
        eprintln!("Sending: {}", out_str);
        println!("{}", out_str);

        Ok(())
    }

    fn broadcast(&mut self, reply: MessageBody) -> Result<()> {
        let nodes = self.node_ids.clone();
        for n in nodes {
            if n == self.id {
                continue;
            }

            self.send(n.to_string(), reply.clone())?;
        }
        Ok(())
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
struct MessageBody {
    #[serde(rename(serialize = "type", deserialize = "type"))]
    msg_type: String,
    msg_id: u128,

    #[serde(default)]
    in_reply_to: u128,

    #[serde(default, skip_serializing)]
    node_id: String,

    #[serde(default, skip_serializing)]
    node_ids: Vec<String>,

    #[serde(default, skip_serializing)]
    topology: HashMap<String, Vec<String>>,

    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    message: u128,

    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    messages: Option<Vec<u128>>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
struct Message {
    src: String,
    dest: String,
    body: MessageBody,
}

#[tokio::main]
async fn  main() -> io::Result<()> {
    let mut node: Node = Node::new();

    loop {
        let mut buffer = String::new();
        io::stdin().read_line(&mut buffer)?;
        eprint!("Received: {}", buffer);

        let msg: Message = serde_json::from_str(&buffer)?;
        let ref body = msg.body;

        let mut reply: MessageBody = Default::default();

        match body.msg_type.as_str() {
            "init" => {
                node.id = body.node_id.to_owned();
                node.node_ids = body.node_ids.to_owned();
                reply.msg_type = "init_ok".to_string();
            },
            "broadcast" => {
                node.messages.insert(body.message);
                reply.msg_type = "broadcast_ok".to_string();

                // TODO: Ideally we batch these up and do them every couple seconds
                node.broadcast(body.clone())?;
            },
            "read" => {
                reply.messages = Some(node.messages.clone().into_iter().collect());
                reply.msg_type = "read_ok".to_string();
            },
            "topology" => {
                reply.msg_type = "topology_ok".to_string();
                node.neighbors = body.topology[&node.id].clone();
                eprintln!("Neighbors set to: {:?}", node.neighbors);
            },
            "broadcast_ok" => {
                continue;
            }
            _ => {
                eprintln!("Unknown message type: {}", body.msg_type);
                continue;
            }
        }

        node.reply(msg, reply)?;
    }
}

/// This is only used for serialize
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero(num: &u128) -> bool {
    *num == 0
}
