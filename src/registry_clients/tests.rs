use super::*;

#[test]
fn test_registry_clients_new() {
    let clients = RegistryClients::new();
    // Verify that a new instance has no clients initially
    assert_eq!(clients.clients.len(), 0);
}
