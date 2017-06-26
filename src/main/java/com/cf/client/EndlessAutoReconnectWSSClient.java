package com.cf.client;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.cf.data.handler.poloniex.PoloniexSubscription;
import com.cf.data.handler.poloniex.PoloniexSubscriptionExceptionHandler;

import rx.functions.Action1;
import ws.wamp.jawampa.ApplicationError;
import ws.wamp.jawampa.PubSubData;
import ws.wamp.jawampa.WampClient;
import ws.wamp.jawampa.WampClientBuilder;
import ws.wamp.jawampa.transport.netty.NettyWampClientConnectorProvider;

public class EndlessAutoReconnectWSSClient implements AutoCloseable {

	private final static Logger LOG = LogManager.getLogger();
	private final WampClient wampClient;
	private final Map<String, Action1<PubSubData>> subscriptions;
	private boolean aborted;
	private boolean wasConnectedBefore = false;

	public EndlessAutoReconnectWSSClient(String uri, String realm) throws ApplicationError, Exception {
		this.subscriptions = new HashMap<>();
		WampClientBuilder builder = new WampClientBuilder();
		builder.withConnectorProvider(new NettyWampClientConnectorProvider()).withUri(uri).withRealm(realm)
				.withInfiniteReconnects().withReconnectInterval(5, TimeUnit.SECONDS);

		wampClient = builder.build();
	}

	public void subscribe(PoloniexSubscription feedEventHandler) {
		this.subscriptions.put(feedEventHandler.feedName, feedEventHandler);
	}

	public void subscribe(String feedName, Action1<PubSubData> feedEventHandler) {
		this.subscriptions.put(feedName, feedEventHandler);
	}

	public void run() {
		try {
			wampClient.statusChanged().subscribe((WampClient.State newState) -> {
				if (newState instanceof WampClient.ConnectedState) {
					LOG.trace("Connected");

					wasConnectedBefore = true;
					for (Entry<String, Action1<PubSubData>> subscription : this.subscriptions.entrySet()) {
						wampClient.makeSubscription(subscription.getKey()).subscribe(subscription.getValue(),
								new PoloniexSubscriptionExceptionHandler(subscription.getKey()));
					}
				} else if (newState instanceof WampClient.DisconnectedState) {

					if (wasConnectedBefore) {
						LOG.trace("Disconnected - try to connect again");
						wampClient.open();
					}
				} else if (newState instanceof WampClient.ConnectingState) {
					LOG.trace("Connecting...");
				}
			});

			wampClient.open();

			while (wampClient.getTerminationFuture().isDone() == false && !aborted) {
				TimeUnit.MINUTES.sleep(1);
			}
		} catch (Exception ex) {
			LOG.error(("Caught exception - " + ex.getMessage()), ex);
		}
	}

	@Override
	public void close() throws Exception {
		aborted = true;
		wampClient.close().toBlocking().last();
	}

}
