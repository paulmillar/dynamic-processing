/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.macroons;

import com.google.gson.Gson;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.time.Duration;

import org.dcache.kafka.streams.Configuration;

/**
 * A client that simplifies requesting macaroons from dCache.
 */
public class MacaroonClient
{
    private class IdentityResponse
    {
        String status;
    }

    private static final ContentType MACAROON_REQUEST = ContentType.create("application/macaroon-request");
    private static final Duration MACAROON_VALIDITY = Duration.ofMinutes(5);

    private final CloseableHttpClient client;
    private final Duration defaultDuration;

    public MacaroonClient(Configuration.Macaroons config, CloseableHttpClient client) throws AuthenticationException, IOException
    {
        this.client = client;
        Duration dl = config.getDefaultLifetime();
        defaultDuration = dl == null ? MACAROON_VALIDITY : dl;

        // FIXME: this doesn't work as the preflight request succeeds.
        //checkCredentials();
    }

    public String getMacaroon(URI uri, String activity)
            throws AuthenticationException, IOException
    {
        return getMacaroon(uri, activity, defaultDuration);
    }

    public String getMacaroon(URI uri, String activity, Duration duration)
            throws AuthenticationException, IOException
    {
        Gson gson = new Gson();
        HttpPost httpPost = new HttpPost(uri);

        // A "path" caveat is added automatically from the POST request path
        MacaroonRequest request = new MacaroonRequest("activity:" + activity);
        request.setValidity(duration);
        String json = gson.toJson(request);
        httpPost.setEntity(new StringEntity(json, MACAROON_REQUEST));

        try (CloseableHttpResponse response = client.execute(httpPost)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Server replied " + response.getStatusLine());
            }
            Reader reader = new InputStreamReader(response.getEntity().getContent(), "UTF-8");
            return gson.fromJson(reader, MacaroonResponse.class).macaroon.trim();
        }
    }

    // REVISIT: seperate class?
    private void checkCredentials() throws AuthenticationException, IOException
    {
        HttpGet httpGet = new HttpGet("https://dcache-xdc.desy.de:3880/api/v1/user");
        try (CloseableHttpResponse response = client.execute(httpGet)) {
            if (response.getStatusLine().getStatusCode() != 200) {
                throw new IOException("Server replied " + response.getStatusLine());
            }
            Reader reader = new InputStreamReader(response.getEntity().getContent(), "UTF-8");
            String status = new Gson().fromJson(reader, IdentityResponse.class).status;
            if (status == null || status.equals("ANONYMOUS")) {
                throw new AuthenticationException("Server rejected username+password: " + response.getEntity());
            }
        }
    }
}
