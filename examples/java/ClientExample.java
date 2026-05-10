import chronos.client.Client;

public final class ClientExample {
  public static void main(String[] args) {
    var config =
        Client.Config.defaults()
            .withTransport(Client.TransportConfig.secure().withPlaintext(true));

    try (Client client =
        new Client(
            "127.0.0.1:50051",
            "orders.primary",
            config)) {
      var ranges = client.allocateTimestamps(1);
      System.out.println("tso=" + ranges.get(0).getStartTso());
    }
  }
}
