import chronos.client.Client;

public final class ClientExample {
  public static void main(String[] args) {
    try (Client client = new Client("127.0.0.1:50051", "orders.primary")) {
      var ranges = client.allocateTimestamps(1);
      System.out.println("tso=" + ranges.get(0).getStartTso());
    }
  }
}
