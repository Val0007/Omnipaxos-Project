use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::io::{self, BufRead, Write};
use std::time::Duration;
use tokio::time::timeout;
use rand::Rng;
use tokio::sync::mpsc;
use tokio::time::sleep;

#[derive(serde::Deserialize, Debug)]
struct ShimResponse {
    ok: bool,
    value: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Body {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    from: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    to: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    text: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Message {
    src: String,
    dest: String,
    body: Body,
}

#[derive(Debug, Clone)]
enum Operation {
    Read(String),
    Write(String, String),
}

#[derive(Debug)]
enum Outcome {
    ReadOk(String),
    WriteOk,
    NotFound,
    Indeterminate,
    Failed(String),
}

#[tokio::main]
async fn main() {
    let stdin = io::stdin();
    let mut node_id = String::new();
    let mut shim_url = String::new();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();

    for line in stdin.lock().lines() {
        let line = line.unwrap();
        if line.trim().is_empty() { continue; }

        let msg: Message = match serde_json::from_str(&line) {
            Ok(m)  => m,
            Err(e) => { eprintln!("[node] parse error: {e}"); continue; }
        };

        match msg.body.msg_type.as_str() {

            "init" => {
                node_id = msg.body.node_id.clone().unwrap();
                shim_url = "http://127.0.0.1:8080".to_string();
                eprintln!("[node] {} → gateway: {}", node_id, shim_url);

                send_reply(&node_id, &msg, Body {
                    msg_type: "init_ok".into(),
                    in_reply_to: msg.body.msg_id,
                    msg_id: None, node_id: None, node_ids: None,
                    key: None, value: None, from: None, to: None,
                    code: None, text: None,
                });
            }

            "read" => {
                let key = msg.body.key.clone().unwrap().to_string();
                let key = key.trim_matches('"').to_string();
                let url = format!("{}/kv/get/{}", shim_url, key);

                let result = timeout(Duration::from_secs(5),
                                     http.get(&url).send()).await;

                let body = match result {
                    Ok(Ok(resp)) => match resp.json::<ShimResponse>().await {
                        Ok(s) if s.ok => {
                            match s.value {
                                None => error_body(msg.body.msg_id, 20, "key not found".into()),
                                Some(raw) => {
                                    let json_value = if let Ok(n) = raw.parse::<i64>() {
                                        Value::Number(serde_json::Number::from(n))
                                    } else {
                                        Value::String(raw)
                                    };
                                    Body {
                                        msg_type: "read_ok".into(),
                                        in_reply_to: msg.body.msg_id,
                                        value: Some(json_value),
                                        msg_id: None, node_id: None, node_ids: None,
                                        key: None, from: None, to: None, code: None, text: None,
                                    }
                                }
                            }
                        },
                        Ok(_) => error_body(msg.body.msg_id, 20, "not found".into()),
                        Err(e) => error_body(msg.body.msg_id, 13, e.to_string()),
                    },
                    Ok(Err(e)) => error_body(msg.body.msg_id, 11, e.to_string()),
                    Err(_)     => error_body(msg.body.msg_id, 11, "timeout".into()),
                };
                send_reply(&node_id, &msg, body);
            }

            "write" => {
                let key = msg.body.key.clone().unwrap().to_string();
                let key = key.trim_matches('"').to_string();
                let value = msg.body.value.clone().unwrap().to_string();
                let value = value.trim_matches('"').to_string();
                let url = format!("{}/kv/put", shim_url);

                let result = timeout(Duration::from_secs(5),
                                     http.post(&url)
                                         .json(&serde_json::json!({"key": key, "value": value}))
                                         .send()).await;

                let body = match result {
                    Ok(Ok(resp)) => match resp.json::<ShimResponse>().await {
                        Ok(s) if s.ok => Body {
                            msg_type: "write_ok".into(),
                            in_reply_to: msg.body.msg_id,
                            msg_id: None, node_id: None, node_ids: None,
                            key: None, value: None, from: None, to: None,
                            code: None, text: None,
                        },
                        Ok(s) => error_body(msg.body.msg_id, 14,
                                            s.error.unwrap_or("write failed".into())),
                        Err(e) => error_body(msg.body.msg_id, 13, e.to_string()),
                    },
                    Ok(Err(e)) => error_body(msg.body.msg_id, 11, e.to_string()),
                    Err(_)     => error_body(msg.body.msg_id, 11, "timeout".into()),
                };
                send_reply(&node_id, &msg, body);
            }

            "cas" => {
    let url = format!("{}/kv/cas", shim_url);
    let result = timeout(Duration::from_secs(5),
        http.post(&url)
            .json(&serde_json::json!({
                "key": key,
                "expected": expected,
                "new_value": new_value
            }))
            .send()).await;

    let body = match result {
        Ok(Ok(resp)) => match resp.json::<ShimResponse>().await {
            Ok(s) if s.ok => Body { msg_type: "cas_ok".into(), ... },
            Ok(s) => error_body(msg.body.msg_id, 22, s.error.unwrap_or("cas failed".into())),
            Err(e) => error_body(msg.body.msg_id, 13, e.to_string()),
        },
        Ok(Err(e)) => error_body(msg.body.msg_id, 11, e.to_string()),
        Err(_) => error_body(msg.body.msg_id, 11, "timeout".into()),
    };
    send_reply(&node_id, &msg, body);
}

            other => eprintln!("[node] unknown type: {other}"),
        }
    }
}

fn send_reply(node_id: &str, original: &Message, body: Body) {
    let reply = Message {
        src: node_id.to_string(),
        dest: original.src.clone(),
        body,
    };
    let json = serde_json::to_string(&reply).unwrap();
    let stdout = io::stdout();
    let mut out = stdout.lock();
    writeln!(out, "{json}").unwrap();
    out.flush().unwrap();
}

fn error_body(in_reply_to: Option<u64>, code: u32, text: String) -> Body {
    Body {
        msg_type: "error".into(),
        in_reply_to,
        code: Some(code),
        text: Some(text),
        msg_id: None, node_id: None, node_ids: None,
        key: None, value: None, from: None, to: None,
    }
}

fn generate_op(rng: &mut impl Rng) -> Operation {
    let key = rng.gen_range(1_u32..=5).to_string();
    let value = rng.gen_range(1_u32..=100).to_string();

    match rng.gen_range(0_u32..10) {
        0..=2 => Operation::Read(key),
        _     => Operation::Write(key, value),
    }
}

async fn execute_op(
    op: &Operation,
    shim_url: &str,
    http: &reqwest::Client,
) -> Outcome {
    match op {

        Operation::Read(key) => {
            let url = format!("{}/kv/get/{}", shim_url, key);
            let result = timeout(
                Duration::from_secs(5),
                http.get(&url).send()
            ).await;

            match result {
                Ok(Ok(resp)) => match resp.json::<ShimResponse>().await {
                    Ok(s) if s.ok => Outcome::ReadOk(s.value.unwrap_or_default()),
                    Ok(_)         => Outcome::NotFound,
                    Err(e)        => Outcome::Failed(e.to_string()),
                },
                Ok(Err(_)) => Outcome::Indeterminate,
                Err(_)     => Outcome::Indeterminate,
            }
        }

        Operation::Write(key, value) => {
            let url = format!("{}/kv/put", shim_url);
            let result = timeout(
                Duration::from_secs(5),
                http.post(&url)
                    .json(&serde_json::json!({"key": key, "value": value}))
                    .send()
            ).await;

            match result {
                Ok(Ok(resp)) => match resp.json::<ShimResponse>().await {
                    Ok(s) if s.ok => Outcome::WriteOk,
                    Ok(s)         => Outcome::Failed(s.error.unwrap_or_default()),
                    Err(e)        => Outcome::Failed(e.to_string()),
                },
                Ok(Err(_)) => Outcome::Indeterminate,
                Err(_)     => Outcome::Indeterminate,
            }
        }
    }
}

async fn run_generator(tx: mpsc::Sender<Operation>) {
    loop {
        let op = {
            let mut rng = rand::thread_rng();
            generate_op(&mut rng)
        };
        eprintln!("[generator] produced: {:?}", op);
        if tx.send(op).await.is_err() { break; }
        sleep(Duration::from_millis(500)).await;
    }
}

async fn run_client(mut rx: mpsc::Receiver<Operation>, shim_url: String) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    while let Some(op) = rx.recv().await {
        let outcome = execute_op(&op, &shim_url, &http).await;
        eprintln!("[client] {:?} → {:?}", op, outcome);
    }
}
