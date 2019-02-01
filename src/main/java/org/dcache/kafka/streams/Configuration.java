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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Data class to hold information about configuration.
 */
public class Configuration
{
    public static class UrlGenerator
    {
        private URI base;
        private Macaroons macaroons;

        public void setBase(URI url)
        {
            base = url;
        }

        public URI getBase()
        {
            return base;
        }

        public void setMacaroons(Macaroons macaroons)
        {
            this.macaroons = macaroons;
        }

        public Macaroons getMacaroons()
        {
            return macaroons;
        }

        public void checkValid()
        {
            checkArgument(base != null, "Missing base");
            checkArgument(base.isAbsolute(), "Bad base: not absolute");
            checkArgument(!base.isOpaque(), "Bad base: is opaque");
        }
    }

    public static class Macaroons
    {
        private String username;
        private String password;
        @JsonProperty("default-lifetime")
        private Duration defaultLifetime = Duration.ofMinutes(5);

        public String getUsername()
        {
            return username;
        }

        public void setUsername(String username)
        {
            this.username = username;
        }

        public String getPassword()
        {
            return password;
        }

        public void setPassword(String password)
        {
            this.password = password;
        }

        public Duration getDefaultLifetime()
        {
            return defaultLifetime;
        }

        public void setDefaultLifetime(String lifetime)
        {
            this.defaultLifetime = Duration.parse(lifetime);
        }

        public void checkValid()
        {
            checkArgument(username != null, "Missing username");
            checkArgument(!username.trim().isEmpty(), "Empty username");
            checkArgument(password != null, "Missing password");
            checkArgument(!password.trim().isEmpty(), "Empty password");
        }
    }

    public static class Client
    {
        private String id;

        @JsonProperty("bootstrap-servers")
        private String bootstrap;

        public void setId(String name)
        {
            this.id = name;
        }

        public String getId()
        {
            return id;
        }

        public void setBootstrap(String endpoints)
        {
            bootstrap = endpoints;
        }

        public String getBootstrap()
        {
            return bootstrap;
        }

        public void checkValid()
        {
            if (id == null) {
                throw new IllegalArgumentException("Missing 'id'");
            }
            if (id.trim().isEmpty()) {
                throw new IllegalArgumentException("Empty 'id'");
            }
        }
    }

    public static class GeneratedUrl
    {
        @JsonProperty("with-macaroon")
        private Map<String,String> withMacaroon;

        @JsonProperty("derived-path")
        private Map<String,String> derivedPath;

        @JsonProperty("create-path")
        private boolean createPath;

        public void setWithMacaroon(Map<String,String> withMacaroon)
        {
            this.withMacaroon = withMacaroon;
        }

        public Map<String,String> getWithMacaroon()
        {
            return withMacaroon;
        }

        public void setDerivedPath(Map<String,String> derivedPath)
        {
            this.derivedPath = derivedPath;
        }

        public Map<String,String> getDerivedPath()
        {
            return derivedPath;
        }

        public void setCreatePath(boolean isPathCreated)
        {
            this.createPath = isPathCreated;
        }

        public boolean isCreatePath()
        {
            return createPath;
        }
    }

    public static class EventSource
    {
        private String target;

        @JsonProperty("path-predicate")
        private Map<String,String> pathPredicate;
        private Map<String,GeneratedUrl> urls;

        public void setTarget(String topic)
        {
            this.target = topic;
        }

        public String getTarget()
        {
            return target;
        }

        public void setPathPredicate(Map<String,String> criteria)
        {
            pathPredicate = criteria;
        }

        public Map<String,String> getPathPredicate()
        {
            return pathPredicate;
        }

        public void setUrls(Map<String,GeneratedUrl> urls)
        {
            this.urls = urls;
        }

        public Map<String,GeneratedUrl> getUrls()
        {
            return urls;
        }
    }

    @JsonProperty("kafka-client")
    private Client client;

    @JsonProperty("url-generator")
    private UrlGenerator urls;

    @JsonProperty("files")
    private List<EventSource> eventSource;

    public Client getClient()
    {
        return client;
    }

    public UrlGenerator getUrlGenerator()
    {
        return urls;
    }

    public List<EventSource> getEventSource()
    {
        return eventSource;
    }

    public void checkValid()
    {
        checkArgument(urls != null, "Missing 'authentication'");
        checkArgument(client != null, "Missing 'client'");
        urls.checkValid();
        client.checkValid();
    }
}
