package com.github.mnuessler.influxdb;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.fluent.Executor;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InfluxDbHcClient implements InfluxDbClient {

    private static final Logger LOG = LoggerFactory.getLogger(InfluxDbHcClient.class);
    private static final ContentType CONTENT_TYPE = ContentType.create(InfluxDbClient.CONTENT_TYPE, InfluxDbClient.CHARSET);

    public static Builder forUrl(String influxDbUrl) {
		return new Builder(influxDbUrl);
	}

	public static class Builder {
		private HttpClient client = HttpClients.createDefault();

		private URI influxDbWriteUrl;

		private Credentials credentials;

		private int socketTimeout = 5;

		private int connectTimeout = 5;

		private Builder(@Nonnull String influxDbUrl) {
			this.influxDbWriteUrl = URI.create(influxDbUrl + "/write").normalize();
		}

		public Builder withCredentials(@Nonnull String username, @Nonnull String password) {
			this.credentials = new UsernamePasswordCredentials(username, password);
			return this;
		}

		public Builder withHttpClient(@Nonnull HttpClient client) {
			this.client = client;
			return this;
		}

		private Builder withSocketTimeout(int socketTimeout) {
			this.socketTimeout = socketTimeout;
			return this;
		}

		private Builder withConnectTimeout(int connectTimeout) {
			this.connectTimeout = connectTimeout;
			return this;
		}

		public InfluxDbClient build() {
			return new InfluxDbHcClient(client, influxDbWriteUrl, credentials, socketTimeout, connectTimeout);
		}

	}

    private final HttpClient client;

	private final URI influxDbWriteUrl;

	private final Credentials credentials;

	private final int socketTimeout;

	private final int connectTimeout;

	private final Precision precision = Precision.MILLISECONDS;

	private InfluxDbHcClient(@Nonnull HttpClient client, @Nonnull URI influxDbWriteUrl, @Nullable Credentials credentials,
			int socketTimeout, int connectTimeout) {
		this.client = client;
		this.influxDbWriteUrl = influxDbWriteUrl;
		this.credentials = credentials;
		this.socketTimeout = socketTimeout;
		this.connectTimeout = connectTimeout;
	}

	/**
	 * <table>
	 * <thead>
	 * <td>HTTP status code</td>
	 * <td>Description</td> </thead>
	 * <tr>
	 * <td>204 No Content</td>
	 * <td>Success!</td>
	 * </tr>
	 * <tr>
	 * <td>400 Bad Request</td>
	 * <td>Unacceptable request. Can occur with a Line Protocol syntax error or if a user attempts to write values to a field that
	 * previously accepted a different value type. The returned JSON offers further information.</td>
	 * </tr>
	 * <tr>
	 * <td>204 No Content</td>
	 * <td>Success!</td>
	 * </tr>
	 * </table>
	 * HTTP status code Description 204 No Content Success! 400 Bad Request Unacceptable request. Can occur with a Line Protocol syntax
	 * error or if a user attempts to write values to a field that previously accepted a different value type. The returned JSON offers
	 * further information. 404 Not Found Unacceptable request. Can occur if a user attempts to write to a database that does not exist. The
	 * returned JSON offers further information. 500 Internal Server Error The system is overloaded or significantly impaired. Can occur if
	 * a user attempts to write to a retention policy that does not exist. The returned JSON offers further information.
	 * 
	 * @param payload
	 * @param database
	 * @param retentionPolicy
	 * @throws IOException
	 */
	@Override
	public void write(@Nonnull CharSequence payload, @Nonnull String database, @Nullable String retentionPolicy) throws IOException {
		URIBuilder builder = new URIBuilder(influxDbWriteUrl).setParameter("db", database).setParameter("precision", precision.getUnit());

		if (retentionPolicy != null && !"default".equalsIgnoreCase(retentionPolicy)) {
			builder.setParameter("rp", retentionPolicy);
		}

		URI targetUrl;
		try {
			targetUrl = builder.build();
		} catch (URISyntaxException e) {
			throw new IOException(e);
		}

        Executor executor = Executor.newInstance(client);
		if (credentials != null) {
			executor.auth(new HttpHost(influxDbWriteUrl.getHost(), influxDbWriteUrl.getPort()), credentials);
		}

        executor.execute(
            Request.Post(targetUrl)
                .socketTimeout(socketTimeout)
                .connectTimeout(connectTimeout)
                .bodyString(payload.toString(), CONTENT_TYPE))
            .handleResponse(new InfluxDbWriteResponseHandler());
	}

	private static class InfluxDbWriteResponseHandler implements ResponseHandler {
		@Override
		public Void handleResponse(final HttpResponse response) throws IOException {
			StatusLine statusLine = response.getStatusLine();
			int statusCode = statusLine.getStatusCode();

			if (statusCode != HttpStatus.SC_NO_CONTENT && statusCode != HttpStatus.SC_OK) {
			    if (LOG.isDebugEnabled()) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        LOG.debug("Response body: {}", EntityUtils.toString(entity, InfluxDbClient.CHARSET));
                    }
                }
				throw new HttpResponseException(statusCode, "InfluxDB server responded with: " + statusLine);
			}
			return null;
		}
	}
}
