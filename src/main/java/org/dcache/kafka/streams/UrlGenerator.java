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
package org.dcache.kafka.streams;

import org.apache.http.auth.AuthenticationException;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;

import org.dcache.macroons.MacaroonClient;

/**
 * Class responsible for generated a URL from the supplied path.
 */
public class UrlGenerator
{
    private final URI base;
    private final MacaroonClient macaroons;

    public UrlGenerator(URI base, MacaroonClient macaroons)
    {
        this.base = base;
        this.macaroons = macaroons;
    }

    public URI url(String path)
    {
        try {
            return new URI(base.getScheme(), base.getAuthority(), path, null, null);
        }   catch (URISyntaxException e) {
            throw new RuntimeException("Bad URL: " + e, e);
        }
    }

    public URI urlWithMacaroon(String path, String activity)
    {
        URI baseUrl = url(path);

        try {
            return addMacaroon(baseUrl, macaroons.getMacaroon(url(path), activity));
        } catch (AuthenticationException | IOException e) {
            System.out.println("Fetching macaroon failed: " + e);
            return baseUrl;
        }

    }

    public URI urlWithMacaroon(String path, String activity, Duration lifetime)
    {
        URI baseUrl = url(path);

        try {
            return addMacaroon(baseUrl, macaroons.getMacaroon(url(path), activity, lifetime));
        } catch (AuthenticationException | IOException e) {
            return baseUrl;
        }

    }

    private URI addMacaroon(URI uri, String macaroon)
    {
        String query = "authz=" + macaroon;
        try {
            return new URI(base.getScheme(), base.getAuthority(), uri.getPath(), query, null);
        }   catch (URISyntaxException e) {
            throw new RuntimeException("Bad URL: " + e, e);
        }
    }
}
