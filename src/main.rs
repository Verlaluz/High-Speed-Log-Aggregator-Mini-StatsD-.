use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{interval, Duration};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    
    let (log_channel_transmitter, mut log_channel_receiver) = mpsc::channel::<String>(5000);

    let aggregator_udp_socket = Arc::new(UdpSocket::bind("0.0.0.0:8125").await.unwrap());
    println!("🚀 UDP Log Aggregator listening on port 8125...");

    
    let network_listener_socket = aggregator_udp_socket.clone();
    tokio::spawn(async move {
        // A buffer to hold the raw bytes of incoming UDP packets
        let mut raw_datagram_buffer = [0u8; 1024];

        loop {
            
            if let Ok((bytes_received_count, client_network_address)) = network_listener_socket.recv_from(&mut raw_datagram_buffer).await {
                
                if let Ok(parsed_log_string) = std::str::from_utf8(&raw_datagram_buffer[..bytes_received_count]) {
                    let formatted_log_entry = format!("[FROM {}]: {}\n", client_network_address, parsed_log_string.trim());
                    
                   
                    let _ = log_channel_transmitter.send(formatted_log_entry).await;
                }
            }
        }
    });

    
    let maximum_batch_size = 5; 
    let mut log_batch_memory_buffer: Vec<String> = Vec::new();
    
  
    let mut flush_interval_timer = interval(Duration::from_secs(5));

    println!("Batching rules: Writing to disk every {} logs OR every 5 seconds.", maximum_batch_size);

    loop {
 
        tokio::select! {

            Some(incoming_log_message) = log_channel_receiver.recv() => {
                log_batch_memory_buffer.push(incoming_log_message);
                
                
                if log_batch_memory_buffer.len() >= maximum_batch_size {
                    println!("📦 Buffer reached {} items. Flushing to disk...", maximum_batch_size);
                    flush_logs_to_disk(&mut log_batch_memory_buffer).await;
                }
            }
            
            
            _ = flush_interval_timer.tick() => {
                
                if !log_batch_memory_buffer.is_empty() {
                    println!("⏱️ 5 seconds passed. Flushing {} lingering items to disk...", log_batch_memory_buffer.len());
                    flush_logs_to_disk(&mut log_batch_memory_buffer).await;
                }
            }
        }
    }
}


async fn flush_logs_to_disk(memory_buffer_reference: &mut Vec<String>) {
    let mut aggregate_log_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("aggregated_server_logs.txt")
        .await
        .expect("Failed to open log file");

    for single_log_entry in memory_buffer_reference.iter() {
        let _ = aggregate_log_file.write_all(single_log_entry.as_bytes()).await;
    }
    
    let _ = aggregate_log_file.flush().await;
    
  
    memory_buffer_reference.clear();
}