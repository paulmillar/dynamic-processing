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

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class UrlGeneratorTemplate
{
    private final UrlGenerator generator;
    private final Map<String,String> macaroon;
    private final Pattern replacing;
    private final String with;
    private final boolean isCreatePath;

    public UrlGeneratorTemplate(Configuration.GeneratedUrl config, UrlGenerator generator)
    {
        this.generator = generator;
        macaroon = config.getWithMacaroon();
        Map<String,String> derivedPath = config.getDerivedPath();
        if (derivedPath == null) {
            replacing = null;
            with = null;
            isCreatePath = false;
        } else {
            String r = derivedPath.get("replacing");
            replacing = (r == null) ? null : Pattern.compile(r);
            with = derivedPath.get("with");
            isCreatePath = config.isCreatePath();
        }
    }

    public boolean isCreatePath()
    {
        return this.isCreatePath;
    }

    public URI buildUrl(String path)
    {
        String targetPath = buildTargetPath(path);

        if (macaroon == null) {
            return generator.url(targetPath);
        } else {
            String activity = macaroon.get("activity");
            if (activity == null) {
                activity = "DOWNLOAD"; // a default?
            }
            String duration = macaroon.get("lifetime");
            if (duration == null) {
                return generator.urlWithMacaroon(targetPath, activity);
            } else {
                return generator.urlWithMacaroon(targetPath, activity, Duration.parse(duration));
            }
        }
    }

    private String buildTargetPath(String newDataPath)
    {
        if (replacing == null || with == null) {
            return newDataPath;
        } else {
            Matcher m = replacing.matcher(newDataPath);
            if (!m.find()) {
                System.out.println("WARNING: no match for " + replacing.pattern());
                return newDataPath;
            } else {
                StringBuilder targetPath = new StringBuilder();
                boolean scanning = false;
                StringBuilder sb = null;
                for (char i : with.toCharArray()) {
                    if (scanning) {
                        if (i >= '0' && i <= '9') {
                            sb.append((char) i);
                        } else {
                            if (sb.length() == 0) {
                                targetPath.append('\\');
                            } else {
                                int index = Integer.parseInt(sb.toString());
                                if (index <= m.groupCount()) {
                                    targetPath.append(m.group(index));
                                } else {
                                    targetPath.append('\\').append(sb);
                                }
                                targetPath.append((char) i);
                                scanning = false;
                            }
                        }
                    } else {
                        if (i == '\\') {
                            sb = new StringBuilder();
                            scanning = true;
                        } else {
                            targetPath.append(i);
                        }
                    }
                }
                if (scanning && sb.length() > 0) {
                    int index = Integer.parseInt(sb.toString());
                    if (index <= m.groupCount()) {
                        targetPath.append(m.group(index));
                    } else {
                        targetPath.append('\\').append(sb);
                    }
                }
                return newDataPath.substring(0, m.start(0)) + targetPath;
            }
        }
    }
}
