import chronos.client.Client;
import com.chronos.tso.v1.ResourceTier;
import com.chronos.tso.v1.TimestampRange;
import java.util.List;

final class PackageConsumer {
  private PackageConsumer() {}

  static Client.Config config() {
    return Client.Config.defaults().withDesiredResourceTier(ResourceTier.RESOURCE_TIER_SHARED);
  }

  static long firstTimestamp(List<TimestampRange> ranges) {
    return ranges.get(0).getStartTso();
  }
}
