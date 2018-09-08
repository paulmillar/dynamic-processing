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
package org.dcache.kafka.streams.billing;

/**
 * A data class used to deserialise Kafka data into a usable form.
 */
public class BillingEvent
{
    String msgType;
    String cellType;
    String transferPath;
    String isWrite;
    boolean isP2p;
    Status status;

    public boolean isEventFromPool()
    {
        return cellType.equals("pool");
    }

    public String getPath()
    {
        return transferPath;
    }

    public boolean isWrite()
    {
        return msgType.equals("transfer") && isWrite.equals("write");
    }

    public boolean isTransferFromClient()
    {
        return !isP2p;
    }

    public boolean isSuccessful()
    {
        return status != null && status.code == 0;
    }

    @Override
    public String toString()
    {
        return "[msgType:" + msgType
                + ", cellType:" + cellType
                + ", transferPath:" + transferPath
                + ", isWrite:" + isWrite
                + ", isP2p:" + isP2p
                + ", status:" + status
                +"]";
    }
}
