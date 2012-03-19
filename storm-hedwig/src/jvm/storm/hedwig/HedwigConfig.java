package storm.hedwig;

import backtype.storm.spout.RawScheme;
import backtype.storm.spout.Scheme;
import java.io.Serializable;
import java.net.URL;
import java.util.List;
import com.google.protobuf.ByteString;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hedwig.client.conf.ClientConfiguration;

public class HedwigConfig implements Serializable {
    // TODO: make final
    public Scheme scheme = new RawScheme();
    public final ClientConfiguration clientConfig;
    public final String topic;
    // TODO: make configurable
    public final int messageReadaheadCount = 16384;

    public HedwigConfig(ClientConfiguration config, String topic) {
        this.clientConfig = config;
        this.topic = topic;
    }

    public static HedwigConfig fromURL(URL configFileLocation, String topic)
            throws ConfigurationException {
        ClientConfiguration cfg = new ClientConfiguration();
        cfg.loadConf(configFileLocation);
        return new HedwigConfig(cfg, topic);
    }
}
