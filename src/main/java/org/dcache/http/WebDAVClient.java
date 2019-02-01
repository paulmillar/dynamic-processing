/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2019 Deutsches Elektronen-Synchrotron
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
package org.dcache.http;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 *  A simple class to handle namespace operations.
 */
public class WebDAVClient
{
    private final CloseableHttpClient client;

    public WebDAVClient(CloseableHttpClient client)
    {
        this.client = client;
    }

    public void mkcol(URI target)
    {
        try {
            URI mkcolTarget = target.getQuery() == null
                    ? target
                    : new URI(target.getScheme(), target.getAuthority(), target.getPath(), null, null);

            HttpUriRequest request = RequestBuilder.create("MKCOL").setUri(mkcolTarget).build();

            try (CloseableHttpResponse response = client.execute(request)) {
                switch (response.getStatusLine().getStatusCode()) {
                case 201: // Created
                    break;

                case 405: // already exists
                    System.out.println("Already exists " + mkcolTarget);
                    break;

                default:
                    System.out.println("Error MKCOL on " + mkcolTarget + " " + response.getStatusLine());
                    break;
                }
            }
        } catch (IOException e) {
            System.out.println("Failed to contact server: " + e);
        } catch (URISyntaxException e) {
            System.out.println("MKCOL request failed: " + e);
        }
    }
}
