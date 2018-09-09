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

import com.google.gson.Gson;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 *
 */
public class EventGenerator
{
    private final String topic;
    private final Pattern matching;
    private final Pattern ignoring;
    private final Map<String,UrlGeneratorTemplate> urlGeneratorTemplates = new HashMap<>();

    public EventGenerator(Configuration.EventSource config, UrlGenerator urlGenerator)
    {
        String matchingRe = config.getPathPredicate().get("matching");
        matching = matchingRe == null ? null : Pattern.compile(matchingRe);

        String ignoringRe = config.getPathPredicate().get("ignoring");
        ignoring = ignoringRe == null ? null : Pattern.compile(ignoringRe);

        for (Map.Entry<String,Configuration.GeneratedUrl> e : config.getUrls().entrySet()) {
            urlGeneratorTemplates.put(e.getKey(), new UrlGeneratorTemplate(e.getValue(), urlGenerator));
        }
        topic = config.getTarget();
    }

    public boolean matches(String path)
    {
        if (matching != null && !matching.matcher(path).matches()) {
            return false;
        }
        if (ignoring != null && ignoring.matcher(path).find()) {
            return false;
        }
        return true;
    }

    public Optional<OutgoingEvent> map(String path)
    {
        if (matches(path)) {
            return Optional.of(buildEvent(path));
        } else {
            return Optional.empty();
        }
    }

    private OutgoingEvent buildEvent(String path)
    {
        Map<String,String> urls = new HashMap<>();

        for (Map.Entry<String, UrlGeneratorTemplate> e : urlGeneratorTemplates.entrySet()) {
            URI url = e.getValue().buildUrl(path);
            urls.put(e.getKey(), url.toASCIIString());
        }

        String payload = new Gson().toJson(urls);
        return new OutgoingEvent(topic, payload);
    }
}
