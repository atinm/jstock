/*
 * JStock - Free Stock Market Software
 * Copyright (C) 2014 Yan Cheng Cheok <yccheok@yahoo.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */

package org.yccheok.jstock.engine;

import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author yccheok
 */
public class RealTimeIndexMonitor extends Subject<RealTimeIndexMonitor, java.util.List<Market>> {
    /** Creates a new instance of RealTimeIndexMonitor */
    public RealTimeIndexMonitor(int maxThread, int maxBucketSize, long delay) {
        realTimeStockMonitor = new RealTimeStockMonitor(maxThread, maxBucketSize, delay);
    }
    
    public synchronized boolean addIndex(Index index) {
        if (realTimeStockMonitor.addStockCode(index.code)) {
            indexMapping.put(index.code, index);
            return true;
        }
        return false;
    }
    
    public synchronized boolean isEmpty() {
        return realTimeStockMonitor.isEmpty();
    }
    
    public synchronized boolean clearIndices() {
        indexMapping.clear();        
        return realTimeStockMonitor.clearStockCodes();
    }
    
    public synchronized boolean removeIndex(Index index) {
        indexMapping.remove(index.code);
        return realTimeStockMonitor.removeStockCode(index.code);
    } 
    
    private final Map<Code, Index> indexMapping = new HashMap<Code, Index>();
    private final RealTimeStockMonitor realTimeStockMonitor;
}