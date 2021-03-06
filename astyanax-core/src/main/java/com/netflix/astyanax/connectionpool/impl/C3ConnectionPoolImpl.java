/*******************************************************************************
 * Copyright 2011 Netflix
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.*;
import com.netflix.astyanax.connectionpool.exceptions.*;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Connection pool implementation using simple round robin. <br/> <br/>
 * It maintains a rotating index over a collection of {@link HostConnectionPool}(s) maintained using a {@link Topology} that reflects the given 
 * partitioned set of pools. Note that the impl uses the <b>pinned host</b> on the operation if it finds one. If there is none, then it uses 
 * all the host connection pools in the topology.
 * 
 * @see {@link RoundRobinExecuteWithFailover} for more details on how failover works with round robin connections.
 * @see {@link Topology} for details on where the collection of {@link HostConnectionPool}(s) are maintained. 
 * @see {@link AbstractHostPartitionConnectionPool} for the base impl of {@link ConnectionPool}
 * 
 * @author elandau
 * 
 * @param <CL>
 */
public class C3ConnectionPoolImpl<CL> extends AbstractHostPartitionConnectionPool<CL> {

    public C3ConnectionPoolImpl(ConnectionPoolConfiguration config, ConnectionFactory<CL> factory,
            ConnectionPoolMonitor monitor) {
        super(config, factory, monitor);
    }

    @SuppressWarnings("unchecked")
    public <R> ExecuteWithFailover<CL, R> newExecuteWithFailover(Operation<CL, R> operation) throws ConnectionException {
        try {
            rebuildPartitions();
            if (operation.getPinnedHost() != null) {
                HostConnectionPool<CL> pool = hosts.get(operation.getPinnedHost());
                if (pool == null) {
                    throw new NoAvailableHostsException("Host " + operation.getPinnedHost() + " not active");
                }
                return new C3ExecuteWithFailover<CL, R>(config, monitor,
                        Arrays.<HostConnectionPool<CL>> asList(pool));
            }
            
            return new C3ExecuteWithFailover<CL, R>(config, monitor, topology.getAllPools().getPools());
        }
        catch (ConnectionException e) {
            monitor.incOperationFailure(e.getHost(), e);
            throw e;
        }
    }
}
