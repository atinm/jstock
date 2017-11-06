/*
 * JStock - Free Stock Market Software
 * Copyright (C) 2015 Yan Cheng Cheok <yccheok@yahoo.com>
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

import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 *
 * @author yccheok
 */
public class EODDataStockHistoryServer implements StockHistoryServer {

    public EODDataStockHistoryServer(Code code) throws StockHistoryNotFoundException {
        this(code, DEFAULT_HISTORY_PERIOD);
    }

    public EODDataStockHistoryServer(Code code, Period period) throws StockHistoryNotFoundException {
        this(code, toBestDuration(period));
        long t0 = this.getTimestamp(0);
        long t1 = this.getTimestamp(this.size() - 1);
        long endTimestamp = Math.max(t0, t1);
        long startTimestamp = period.getStartTimestamp(endTimestamp);
        for (int i = 0, size = timestamps.size(); i < size; i++) {
            long timestamp = timestamps.get(i);
            if (startTimestamp > timestamp) {
                historyDatabase.remove(timestamp);
                timestamps.remove(i);
                size--;
                i--;
                continue;
            }
            break;
        }
    }

    private static Duration toBestDuration(Period period) {
        // 7 is for tolerance. Tolerance is in a way such that : Today is N days. However, we only
        // have latest data, which date is (N-tolerance) days.
        return Duration.getTodayDurationByPeriod(period).backStepStartDate(7);
    }

    public EODDataStockHistoryServer(Code code, Duration duration) throws StockHistoryNotFoundException {
        this.code = code;
        this.duration = duration;
        Boolean success = false;

        historyDatabase.clear();
        timestamps.clear();

        final long startTimeInMilli = this.duration.getStartDate().getTime().getTime();
        final long endTimeInMilli = this.duration.getEndDate().getTime().getTime();

        double previousClosePrice = Double.MAX_VALUE;
        long date = 0;

        Symbol symbol = Symbol.newInstance(code.toString());
        String name = symbol.toString();
        Board board = Board.Unknown;
        Industry industry = Industry.Unknown;
        Calendar calendar = Calendar.getInstance();

        // Atin: offset from New York since eoddata is New York time.
        long TIMEZONE_OFFSET = 0;

        //Stock s = stockServer.getStock(code);
        //symbol = s.symbol;
        //name = s.getName();
        //board = s.getBoard();
        //industry = s.getIndustry();
        int startDate = duration.getStartDate().getYear() * 10000
                + duration.getStartDate().getMonth() * 100
                + duration.getStartDate().getDate();
        int endDate = duration.getEndDate().getYear() * 10000
                + duration.getEndDate().getMonth() * 100
                + duration.getEndDate().getDate();

        Connection conn = this.connect();
        if (conn == null) {
            log.error("Could not open database, no history");
            return;
        }

        String description = "SELECT name FROM symbols WHERE symbol = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(description)) {
            // set the values
            pstmt.setString(1, code.toString());
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                name = rs.getString("name") + " (" + symbol.toString() + ")";
                break;
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
        }

        String history = "SELECT date, open, high, low, close, volume "
                + "FROM daily WHERE symbol = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(history)) {

            // set the values
            pstmt.setString(1, code.toString());

            //
            ResultSet rs = pstmt.executeQuery();

            // loop through the result set
            while (rs.next()) {
                log.debug(rs.getDouble("open") + "\t"
                        + rs.getDouble("high") + "\t"
                        + rs.getDouble("low") + "\t"
                        + rs.getDouble("close") + "\t"
                        + rs.getLong("volume"));
                date = rs.getLong("date");
                int year = (int) date / 10000;
                int month = (int) (date - (year * 10000)) / 100;
                int day = (int) (date - (year * 10000) - (month * 100));
                DateTime dt = new DateTime(year, month, day, 0, 0, 0, 0, DateTimeZone.forID("America/New_York"));
                // Convert it to local time respect to stock exchange.
                // TIMEZONE_OFFSET is in minute.
                long currentTimeInMilli = dt.getMillis();

                // Make it as local timestamp.
                //
                // For instance, Greenwich is 1:30pm right now.
                // We want to make Malaysia 1:30pm right now.
                //
                // That's why we are having -ve.
                calendar.setTimeInMillis(currentTimeInMilli);
                int offset = -(calendar.get(Calendar.ZONE_OFFSET) + calendar.get(Calendar.DST_OFFSET));
                currentTimeInMilli = currentTimeInMilli + offset;

                if (currentTimeInMilli < startTimeInMilli) {
                    continue;
                }
                if (currentTimeInMilli > endTimeInMilli) {
                    break;
                }

                double closePrice = 0.0;
                double highPrice = 0.0;
                double lowPrice = 0.0;
                double prevPrice = 0.0;
                double openPrice = 0.0;
                // TODO: CRITICAL LONG BUG REVISED NEEDED.
                long volume = 0;
                //double adjustedClosePrice = 0.0;

                try {
                    closePrice = rs.getDouble("close");
                    highPrice = rs.getDouble("high");
                    lowPrice = rs.getDouble("low");
                    openPrice = rs.getDouble("open");
                    prevPrice = (previousClosePrice == Double.MAX_VALUE) ? 0 : previousClosePrice;

                    // TODO: CRITICAL LONG BUG REVISED NEEDED.
                    volume = rs.getLong("volume");
                    //adjustedClosePrice = Double.parseDouble(fields[6]);
                } catch (NumberFormatException exp) {
                    log.error(null, exp);
                }

                double changePrice = (previousClosePrice == Double.MAX_VALUE) ? 0 : closePrice - previousClosePrice;
                double changePricePercentage = ((previousClosePrice == Double.MAX_VALUE) || (previousClosePrice == 0.0)) ? 0 : changePrice / previousClosePrice * 100.0;

                Stock stock = new Stock(
                        code,
                        symbol,
                        name,
                        null,
                        board,
                        industry,
                        prevPrice,
                        openPrice,
                        closePrice, /* Last Price. */
                        highPrice,
                        lowPrice,
                        volume,
                        changePrice,
                        changePricePercentage,
                        0,
                        0.0,
                        0,
                        0.0,
                        0,
                        0.0,
                        0,
                        0.0,
                        0,
                        0.0,
                        0,
                        0.0,
                        0,
                        currentTimeInMilli
                );

                historyDatabase.put(currentTimeInMilli, stock);

                // Something we do not understand EODData server.
                //if (timestamps.isEmpty()) {
                timestamps.add(currentTimeInMilli);
                //} else {
                //    if (timestamps.get(timestamps.size() - 1) != currentTimeInMilli) {
                //        timestamps.add(currentTimeInMilli);
                //    }
                //}
                previousClosePrice = closePrice;
            }
        } catch (SQLException e) {
            log.error(e.getMessage());
        }

        success = (historyDatabase.size() > 0);
        if (success == false) {
            throw new StockHistoryNotFoundException(code.toString());
        }
    }

    private Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:/Volumes/Data/MarketData/db/data.db";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            log.error(e.getMessage());
        }
        return conn;
    }

    @Override
    public Stock getStock(long timestamp) {
        return historyDatabase.get(timestamp);
    }

    @Override
    public long getTimestamp(int index) {
        return timestamps.get(index);
    }

    @Override
    public int size() {
        return timestamps.size();
    }

    @Override
    public long getSharesIssued() {
        return 0;
    }

    @Override
    public long getMarketCapital() {
        return 0;
    }

    static void setFinalStatic(Field field) throws Exception {
        field.setAccessible(true);

        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
    }

    // I believe EODData server is much more reliable than Yahoo! server.
    private static final int NUM_OF_RETRY = 1;
    private static final Period DEFAULT_HISTORY_PERIOD = Period.Years5;
    private final java.util.Map<Long, Stock> historyDatabase = new HashMap<Long, Stock>();
    private final java.util.List<Long> timestamps = new ArrayList<Long>();
    private final Code code;
    private final StockServer stockServer = GoogleStockServerFactory.newInstance().getStockServer();
    private final Duration duration;

    private static final Log log = LogFactory.getLog(EODDataStockHistoryServer.class);

//    static {
//        // Atin:
//        // private static final Map<Class<? extends StockServerFactory>, PriceSource> classToPriceSourceMap = new HashMap<>(); to static
//        // change org.yccheok.jstock.engine.Utils.classToPriceSourceMap to static
//        Boolean loaded = false;
//        try {
//            Field cp = org.yccheok.jstock.engine.Utils.class.getDeclaredField("classToPriceSourceMap");
//            setFinalStatic(cp);
//            Map classToPriceSourceMap = (java.util.Map) cp.get(null);
//            classToPriceSourceMap.put(EODDataStockServerFactory.class, PriceSource.EODData);
//            loaded = true;
//        }
//        catch (NoSuchFieldException ex) {
//            log.error(null, ex);
//        }
//        catch (Exception ex) {
//            log.error(null, ex);
//        }
//        finally {
//            if (loaded) {
//                log.debug("Successfully add EODDataStockServerFactory");
//            } else {
//                log.error("Could not add EODDataStockServerFactory");
//            }
//        }
//
//        final Set<Class<? extends StockServerFactory>> eoddataSet = new HashSet<>();
//        eoddataSet.add(EODDataStockServerFactory.class);
//
//        // private static final Map<PriceSource, Set<Class<? extends StockServerFactory>>> priceSourceMap = new EnumMap<>(PriceSource.class);
//        // priceSourceMap.put(PriceSource.EODData, eoddataSet);
//        try {
//            Field psm = org.yccheok.jstock.engine.Factories.class.getDeclaredField("priceSourceMap");
//            setFinalStatic(psm);
//            Map priceSourceMap = (java.util.Map) psm.get(null);
//            priceSourceMap.put(PriceSource.EODData, eoddataSet);
//            loaded = true;
//        }
//        catch (NoSuchFieldException ex) {
//            log.error(null, ex);
//        }
//        catch (Exception ex) {
//            log.error(null, ex);
//        }
//        finally {
//            if (loaded) {
//                log.debug("Successfully added EODData, eoddataSet to priceSourceMap");
//            } else {
//                log.error("Could not add EODData, eoddataSet to priceSourceMap");
//            }
//        }
//
//        // final List<StockServerFactory> unitedStateList;
//        // unitedStateList.add(EODDataStockServerFactory.newInstance());
//        List unitedStateList = null;
//        try {
//            Field usl = org.yccheok.jstock.engine.Factories.class.getDeclaredField("unitedStateList");
//            setFinalStatic(usl);
//            unitedStateList = (java.util.List) usl.get(null);
//            unitedStateList.add(EODDataStockServerFactory.newInstance());
//            loaded = true;
//            // private static final Map<Country, List<StockServerFactory>> map = new EnumMap<>(Country.class);
//            // map.put(Country.UnitedState, unitedStateList);
//            Field m = org.yccheok.jstock.engine.Factories.class.getDeclaredField("map");
//            setFinalStatic(m);
//            Map map = (java.util.Map) m.get(null);
//            map.put(Country.UnitedState, unitedStateList);
//            loaded = true;
//        }
//        catch (NoSuchFieldException ex) {
//            log.error(null, ex);
//        }
//        catch (Exception ex) {
//            log.error(null, ex);
//        }
//        finally {
//            if (loaded) {
//                log.debug("Successfully added EODDataStockServerFactory.newInstance() to unitedStateList");
//            } else {
//                log.error("Could not add EODDataStockServerFactory.newInstance() to unitedStateList");
//            }
//        }
//
//        if (loaded) {
//            try {
//                // private static final Map<Country, List<StockServerFactory>> map = new EnumMap<>(Country.class);
//                // map.put(Country.UnitedState, unitedStateList);
//                Field m = org.yccheok.jstock.engine.Factories.class.getDeclaredField("map");
//                setFinalStatic(m);
//                Map map = (java.util.Map) m.get(null);
//                map.put(Country.UnitedState, unitedStateList);
//                loaded = true;
//                }
//            catch (NoSuchFieldException ex) {
//                log.error(null, ex);
//            }
//            catch (Exception ex) {
//                log.error(null, ex);
//            }
//            finally {
//                if (loaded) {
//                    log.debug("Successfully added unitedStateList to map");
//                } else {
//                    log.error("Could not add unitedStateList to map");
//                }
//            }
//        }
//    }
}
