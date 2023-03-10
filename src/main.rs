use std::io;
use serde::{Deserialize, Serialize};
use serde_json::Result;

#[derive(Default, Debug)]
struct Node {
    id: String,
    msg_id: u128,
    node_ids: Vec<String>,
    messages: Vec<u128>,
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
}

#[derive(Default, Serialize, Deserialize, Debug)]
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

    #[serde(default)]
    #[serde(skip_serializing_if = "is_zero")]
    message: u128,

    #[serde(default)]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    messages:Vec<u128>,
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
                node.messages.push(body.message);
                reply.msg_type = "broadcast_ok".to_string();
            },
            "read" => {
                reply.messages = node.messages.clone();
                reply.msg_type = "read_ok".to_string();
            },
            "topology" => {
                reply.msg_type = "topology_ok".to_string();
            },
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
