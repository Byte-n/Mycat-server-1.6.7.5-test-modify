/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.server;

import io.mycat.MycatServer;
import io.mycat.backend.BackendConnection;
import io.mycat.backend.datasource.PhysicalDBNode;
import io.mycat.backend.mysql.nio.handler.*;
import io.mycat.config.ErrorCode;
import io.mycat.config.MycatConfig;
import io.mycat.config.model.SchemaConfig;
import io.mycat.config.model.TableConfig;
import io.mycat.net.FrontendConnection;
import io.mycat.net.mysql.OkPacket;
import io.mycat.route.RouteResultset;
import io.mycat.route.RouteResultsetNode;
import io.mycat.server.parser.ServerParse;
import io.mycat.server.sqlcmd.SQLCmdConstant;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

/**
 * @author mycat
 * @author mycat
 */
public class NonBlockingSession implements Session {

    public static final Logger LOGGER = LoggerFactory.getLogger(NonBlockingSession.class);

    private final ServerConnection source;
    //huangyiming add 避免出现jdk版本冲突
    private final ConcurrentMap<RouteResultsetNode, BackendConnection> target;
    private final MultiNodeCoordinator multiNodeCoordinator;
    private final CommitNodeHandler commitHandler;
    // life-cycle: each sql execution
    private volatile SingleNodeHandler singleNodeHandler;
    private volatile MultiNodeQueryHandler multiNodeHandler;
    private volatile RollbackNodeHandler rollbackHandler;
    private volatile String xaTXID;

    //huangyiming
    private volatile boolean canClose = true;

    private volatile MiddlerResultHandler middlerResultHandler;
    private boolean prepared;

    public NonBlockingSession(ServerConnection source) {
        this.source = source;
        this.target = new ConcurrentHashMap<RouteResultsetNode, BackendConnection>(2, 0.75f);
        multiNodeCoordinator = new MultiNodeCoordinator(this);
        commitHandler = new CommitNodeHandler(this);
    }

    public MultiNodeQueryHandler getMultiNodeHandler() {
        return multiNodeHandler;
    }

    @Override
    public ServerConnection getSource() {
        return source;
    }

    @Override
    public int getTargetCount() {
        return target.size();
    }

    public Set<RouteResultsetNode> getTargetKeys() {
        return target.keySet();
    }

    public BackendConnection getTarget(RouteResultsetNode key) {
        return target.get(key);
    }

    public Map<RouteResultsetNode, BackendConnection> getTargetMap() {
        return this.target;
    }

    public BackendConnection removeTarget(RouteResultsetNode key) {
        return target.remove(key);
    }

    /**
     * 在execute(RouteResultset , int )的基础上，单点查询时，记录表格名称到路由结果中。
     * @param rrs
     * @param type
     * @param schema
     */
    public void execute(RouteResultset rrs, int type, SchemaConfig schema) {
        //调用原本的方法
        execute(rrs, type);
        //即使是单节点查询，也记录当前查询的表格到RouteResultset中
        if (singleNodeHandler != null) {
            //获取路由结果
            RouteResultset routeResultset = singleNodeHandler.getRouteResultset();
            //获取所有路由节点
            RouteResultsetNode[] nodes = routeResultset.getNodes();
            if (nodes.length < 1) {//没有路由节点直接返回，不作处理
                return;
            }
            //获取SchemaConfig中的表格
            Map<String, TableConfig> ts = schema.getTables();
            if (ts == null) {//为空，直接返回，不作处理
                return;
            }
            //单节点查询，就一个节点
            String nodeName = nodes[0].getName();
            //获取当前路由结果的表格
            List<String> tables = routeResultset.getTables();
            if (tables == null) {//为空，则创建一个
                tables = new ArrayList<>();
                routeResultset.setTables(tables);
            }
            //当前查询的SQL语句
            String sql = routeResultset.getStatement().toUpperCase();
            //遍历SchemaConfig中的所有表格
            for (TableConfig v : ts.values()) {
                //对应的节点为空，则直接进行下一次遍历
                if (v.getDataNodes().size() < 1) {
                    continue;
                }
                //遍历该表格对应的节点
                for (String dN : v.getDataNodes()) {
                    //和当前节点相同,且字符串中有该表格，则加入到当前查询的表格中
                    if (dN.equals(nodeName) && containsTableName(sql,v.getName().toUpperCase())) {
                        if (!tables.contains(v.getName())){
                            tables.add(v.getName());
                        }
                    }
                }
            }
        }
    }
    /**
     * 判断sql里面是否包含指定的tableName（不包括注释里面的,区分大小写）
     *
     * @param sql       SQL
     * @param tableName 表格名称
     * @return true:包含
     */
    public boolean containsTableName(String sql, String tableName) {
        //注释对应的索引下标
        HashMap<Integer, Integer> noteIndexMap = new HashMap<>();
        //表格名称对应的索引下标
        ArrayList<Integer> tableIndexMap = new ArrayList<>();
        //注释开始索引
        int nStart = sql.indexOf("/*");
        //注释结束索引
        int nEnd = sql.indexOf("*/");
        //没有注解，就可以直接调用contains判断
        if (nStart == -1) {
            //匹配：
            //表格两边有空格 " tableName "
            //表格后端没空格（sql结尾） " tableName"
            //表格后端带';'（sql结尾带分号） " tableName;"
            //表格后端带'/*'（sql结尾带注释） " tableName/*"。
            // （sql表格名称后面直接跟/**/，不会被当前版本的MyCat（1.6.7.5-test）正确解析，也不会运行到这里（也就是，不会发起后端查询），但这里也注明这种条件）
            return Pattern.matches(".*\\s+" + tableName + "($|\\s.*|/*$.*|;$.*)", sql);
        }
        //添加到集合中
        noteIndexMap.put(nStart, nEnd);
        //循环，是否有多个注解
        for (; ; ) {
            nStart = sql.indexOf("/*", nStart + 1);
            if (nStart == -1) {
                break;
            }
            nEnd = sql.indexOf("*/", nStart);
            noteIndexMap.put(nStart, nEnd);
        }
        //表格索引
        int tStart = sql.indexOf(tableName);
        if (tStart == -1) {
            return false;
        }
        tableIndexMap.add(tStart);
        for (; ; ) {
            tStart = sql.indexOf(tableName, tStart + 1);
            if (tStart == -1) {
                break;
            }
            tableIndexMap.add(tStart);
        }
        ArrayList<Integer> removeIndex = new ArrayList<>();
        //遍历所有表格的索引
        for (Integer i : tableIndexMap) {
            //遍历注解索引
            for (Integer s : noteIndexMap.keySet()) {
                //判断表格是否在注释里面
                if (i > s && (i + tableName.length()) <= noteIndexMap.get(s)) {
                    //移除此表格的索引
                    removeIndex.add(i);
                }
            }
        }
        tableIndexMap.removeAll(removeIndex);
        if (tableIndexMap.isEmpty()) {
            return false;
        }
        //向前，后多截取一位
        int tbs = tableIndexMap.get(0) - 1;
        int tbe = tbs + tableName.length() + 2;
        //可能不能向后多截取一位
        if (tbe >= sql.length()) {
            tbe = sql.length();
        }
        String substring = sql.substring(tbs, tbe);
        //进行判定的字符串
        System.out.println("进行判定的字符串" + substring);
        //匹配
        //表格两边有空格 " tableName "
        // 表格后端没空格（sql结尾） " tableName"
        // 表格后端带';'（sql结尾带分号） " tableName;"
        // 表格后端带'/'（sql结尾带注释） " tableName/"
        // （sql表格名称后面直接跟/**/，不会被当前版本的MyCat（1.6.7.5-test）正确解析，也不会运行到这里（也就是，不会发起后端查询），但这里也注明这种条件）
        boolean matches = Pattern.matches("\\s+" + tableName + "(\\s?|/$|;$)", substring);
        System.out.println("matches = " + matches);
        return matches;
    }

    @Override
    public void execute(RouteResultset rrs, int type) {

        // clear prev execute resources
        clearHandlesResources();
        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder();
            LOGGER.debug(s.append(source).append(rrs).toString() + " rrs ");
        }

        // 检查路由结果是否为空
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0 || nodes[0].getName() == null || nodes[0].getName().equals("")) {
            source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
                    "No dataNode found ,please check tables defined in schema:" + source.getSchema());
            return;
        }
        boolean autocommit = source.isAutocommit();
        final int initCount = target.size();
        if (nodes.length == 1) {
            singleNodeHandler = new SingleNodeHandler(rrs, this);
            if (this.isPrepared()) {
                singleNodeHandler.setPrepared(true);
            }

            try {
                if (initCount > 1) {
                    checkDistriTransaxAndExecute(rrs, 1, autocommit);
                } else {
                    singleNodeHandler.execute();
                }
            } catch (Exception e) {
                LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
                source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }

        } else {

            multiNodeHandler = new MultiNodeQueryHandler(type, rrs, autocommit, this);
            if (this.isPrepared()) {
                multiNodeHandler.setPrepared(true);
            }
            try {
                if (((type == ServerParse.DELETE || type == ServerParse.INSERT || type == ServerParse.UPDATE) && !rrs.isGlobalTable() && nodes.length > 1) || initCount > 1) {
                    checkDistriTransaxAndExecute(rrs, 2, autocommit);
                } else {
                    multiNodeHandler.execute();
                }
            } catch (Exception e) {
                LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
                source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
            }
        }

        if (this.isPrepared()) {
            this.setPrepared(false);
        }
    }

    private void checkDistriTransaxAndExecute(RouteResultset rrs, int type, boolean autocommit) throws Exception {
        switch (MycatServer.getInstance().getConfig().getSystem().getHandleDistributedTransactions()) {
            case 1:
                source.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Distributed transaction is disabled!");
                if (!autocommit) {
                    source.setTxInterrupt("Distributed transaction is disabled!");
                }
                break;
            case 2:
                LOGGER.warn("Distributed transaction detected! RRS:" + rrs);
                if (type == 1) {
                    singleNodeHandler.execute();
                } else {
                    multiNodeHandler.execute();
                }
                break;
            default:
                if (type == 1) {
                    singleNodeHandler.execute();
                } else {
                    multiNodeHandler.execute();
                }
        }
    }

    private void checkDistriTransaxAndExecute() {
        if (!isALLGlobal()) {
            switch (MycatServer.getInstance().getConfig().getSystem().getHandleDistributedTransactions()) {
                case 1:
                    source.writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Distributed transaction is disabled!Please rollback!");
                    source.setTxInterrupt("Distributed transaction is disabled!");
                    break;
                case 2:
                    multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);
                    LOGGER.warn("Distributed transaction detected! Targets:" + target);
                    break;
                default:
                    multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);

            }
        } else {
            multiNodeCoordinator.executeBatchNodeCmd(SQLCmdConstant.COMMIT_CMD);
        }
    }

    public void commit() {
        final int initCount = target.size();
        if (initCount <= 0) {
            ByteBuffer buffer = source.allocate();
            buffer = source.writeToBuffer(OkPacket.OK, buffer);
            source.write(buffer);
            /* 1. 如果开启了 xa 事务 */
            if (getXaTXID() != null) {
                setXATXEnabled(false);
            }
            /* 2. preAcStates 为true,事务结束后,需要设置为true。preAcStates 为ac上一个状态    */
            if (source.isPreAcStates() && !source.isAutocommit()) {
                source.setAutocommit(true);
            }
            return;
        } else if (initCount == 1) {
            //huangyiming add 避免出现jdk版本冲突
            BackendConnection con = target.values().iterator().next();
            commitHandler.commit(con);
        } else {

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("multi node commit to send ,total " + initCount);
            }
            checkDistriTransaxAndExecute();
        }

    }

    private boolean isALLGlobal() {
        for (RouteResultsetNode routeResultsetNode : target.keySet()) {
            if (routeResultsetNode.getSource() == null) {
                return false;
            } else if (!routeResultsetNode.getSource().isGlobalTable()) {
                return false;
            }
        }
        return true;
    }

    public void rollback() {
        final int initCount = target.size();
        if (initCount <= 0) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("no session bound connections found ,no need send rollback cmd ");
            }
            ByteBuffer buffer = source.allocate();
            buffer = source.writeToBuffer(OkPacket.OK, buffer);
            source.write(buffer);
            /* 1. 如果开启了 xa 事务 */
            if (getXaTXID() != null) {
                setXATXEnabled(false);
            }
            /* 2. preAcStates 为true,事务结束后,需要设置为true。preAcStates 为ac上一个状态    */
            if (source.isPreAcStates() && !source.isAutocommit()) {
                source.setAutocommit(true);
            }
            return;
        }

        rollbackHandler = new RollbackNodeHandler(this);
        rollbackHandler.rollback();
    }

    /**
     * 执行lock tables语句方法
     *
     * @param rrs
     * @author songdabin
     * @date 2016-7-9
     */
    public void lockTable(RouteResultset rrs) {
        // 检查路由结果是否为空
        RouteResultsetNode[] nodes = rrs.getNodes();
        if (nodes == null || nodes.length == 0 || nodes[0].getName() == null
                || nodes[0].getName().equals("")) {
            source.writeErrMessage(ErrorCode.ER_NO_DB_ERROR,
                    "No dataNode found ,please check tables defined in schema:"
                            + source.getSchema());
            return;
        }
        LockTablesHandler handler = new LockTablesHandler(this, rrs);
        source.setLocked(true);
        try {
            handler.execute();
        } catch (Exception e) {
            LOGGER.warn(new StringBuilder().append(source).append(rrs).toString(), e);
            source.writeErrMessage(ErrorCode.ERR_HANDLE_DATA, e.toString());
        }
    }

    /**
     * 执行unlock tables语句方法
     *
     * @param sql
     * @author songdabin
     * @date 2016-7-9
     */
    public void unLockTable(String sql) {
        UnLockTablesHandler handler = new UnLockTablesHandler(this, this.source.isAutocommit(), sql);
        handler.execute();
    }

    @Override
    public void cancel(FrontendConnection sponsor) {

    }

    /**
     * {@link ServerConnection#isClosed()} must be true before invoking this
     */
    public void terminate() {
        for (BackendConnection node : target.values()) {
            node.close("client closed ");
        }
        target.clear();
        clearHandlesResources();
    }

    public void closeAndClearResources(String reason) {
        for (BackendConnection node : target.values()) {
            node.close(reason);
        }
        target.clear();
        clearHandlesResources();
    }

    public void releaseConnectionIfSafe(BackendConnection conn, boolean debug,
                                        boolean needRollback) {
        RouteResultsetNode node = (RouteResultsetNode) conn.getAttachment();

        if (node != null) {
            /*  分表 在
             *    1. 没有开启事务
             *    2. 读取走的从节点
             *    3. 没有执行过更新sql
             *    也需要释放连接
             */
//            if (node.isDisctTable()) {
//                return;
//            }
            if (MycatServer.getInstance().getConfig().getSystem().isStrictTxIsolation()) {
                // 如果是严格隔离级别模式的话,不考虑是否已经执行了modifiedSql,直接不释放连接
                if ((!this.source.isAutocommit() && !conn.isFromSlaveDB()) || this.source.isLocked()) {
                    return;
                }
            } else {
                if ((this.source.isAutocommit() || conn.isFromSlaveDB()
                        || !conn.isModifiedSQLExecuted()) && !this.source.isLocked()) {
                    releaseConnection((RouteResultsetNode) conn.getAttachment(), LOGGER.isDebugEnabled(),
                            needRollback);
                }
            }
        }
    }

    public void releaseConnection(RouteResultsetNode rrn, boolean debug,
                                  final boolean needRollback) {

        BackendConnection c = target.remove(rrn);
        if (c != null) {
            if (debug) {
                LOGGER.debug("release connection " + c);
            }
            if (c.getAttachment() != null) {
                c.setAttachment(null);
            }
            if (!c.isClosedOrQuit()) {
                if (c.isAutocommit()) {
                    c.release();
                } else
                //if (needRollback)
                {
                    c.setResponseHandler(new RollbackReleaseHandler());
                    c.rollback();
                }
                //else {
                //	c.release();
                //}
            }
        }
    }

    public void releaseConnections(final boolean needRollback) {
        boolean debug = LOGGER.isDebugEnabled();

        for (RouteResultsetNode rrn : target.keySet()) {
            releaseConnection(rrn, debug, needRollback);
        }
    }

    public void releaseConnection(BackendConnection con) {
        Iterator<Entry<RouteResultsetNode, BackendConnection>> itor = target
                .entrySet().iterator();
        while (itor.hasNext()) {
            BackendConnection theCon = itor.next().getValue();
            if (theCon == con) {
                itor.remove();
                con.release();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("realse connection " + con);
                }
                break;
            }
        }

    }

    /**
     * @return previous bound connection
     */
    public BackendConnection bindConnection(RouteResultsetNode key,
                                            BackendConnection conn) {
        // System.out.println("bind connection "+conn+
        // " to key "+key.getName()+" on sesion "+this);
        return target.put(key, conn);
    }

    public boolean tryExistsCon(final BackendConnection conn, RouteResultsetNode node) {
        if (conn == null) {
            return false;
        }

        boolean canReUse = false;
        // conn 是 slave db 的，并且 路由结果显示，本次sql可以重用该 conn
        if (conn.isFromSlaveDB() && (node.canRunnINReadDB(getSource().isAutocommit())
                && (node.getRunOnSlave() == null || node.getRunOnSlave()))) {
            canReUse = true;
        }

        // conn 是 master db 的，并且路由结果显示，本次sql可以重用该conn
        if (!conn.isFromSlaveDB() && (node.getRunOnSlave() == null || !node.getRunOnSlave())) {
            canReUse = true;
        }

        if (canReUse) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("found connections in session to use " + conn
                        + " for " + node);
            }
            conn.setAttachment(node);
            return true;
        } else {
            // Previous connection and can't use anymore ,release it
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn("Release previous connection,can't be used in trasaction  "
                        + conn + " for " + node);
            }
            releaseConnection(node, LOGGER.isDebugEnabled(), false);
        }
        return false;
    }

//	public boolean tryExistsCon(final BackendConnection conn,
//			RouteResultsetNode node) {
//
//		if (conn == null) {
//			return false;
//		}
//		if (!conn.isFromSlaveDB()
//				|| node.canRunnINReadDB(getSource().isAutocommit())) {
//			if (LOGGER.isDebugEnabled()) {
//				LOGGER.debug("found connections in session to use " + conn
//						+ " for " + node);
//			}
//			conn.setAttachment(node);
//			return true;
//		} else {
//			// slavedb connection and can't use anymore ,release it
//			if (LOGGER.isDebugEnabled()) {
//				LOGGER.debug("release slave connection,can't be used in trasaction  "
//						+ conn + " for " + node);
//			}
//			releaseConnection(node, LOGGER.isDebugEnabled(), false);
//		}
//		return false;
//	}

    protected void kill() {
        boolean hooked = false;
        AtomicInteger count = null;
        Map<RouteResultsetNode, BackendConnection> killees = null;
        for (RouteResultsetNode node : target.keySet()) {
            BackendConnection c = target.get(node);
            if (c != null) {
                if (!hooked) {
                    hooked = true;
                    killees = new HashMap<RouteResultsetNode, BackendConnection>();
                    count = new AtomicInteger(0);
                }
                killees.put(node, c);
                count.incrementAndGet();
            }
        }
        if (hooked) {
            for (Entry<RouteResultsetNode, BackendConnection> en : killees
                    .entrySet()) {
                KillConnectionHandler kill = new KillConnectionHandler(
                        en.getValue(), this);
                MycatConfig conf = MycatServer.getInstance().getConfig();
                PhysicalDBNode dn = conf.getDataNodes().get(
                        en.getKey().getName());
                try {
                    dn.getConnectionFromSameSource(null, true, en.getValue(),
                            kill, en.getKey());
                } catch (Exception e) {
                    LOGGER.error(
                            "get killer connection failed for " + en.getKey(),
                            e);
                    kill.connectionError(e, null);
                }
            }
        }
    }

    private void clearHandlesResources() {
        SingleNodeHandler singleHander = singleNodeHandler;
        if (singleHander != null) {
            singleHander.clearResources();
            singleNodeHandler = null;
        }
        MultiNodeQueryHandler multiHandler = multiNodeHandler;
        if (multiHandler != null) {
            multiHandler.clearResources();
            multiNodeHandler = null;
        }
    }

    public void clearResources(final boolean needRollback) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("clear session resources " + this);
        }
        this.releaseConnections(needRollback);
        clearHandlesResources();
    }

    public boolean closed() {
        return source.isClosed();
    }

    private String genXATXID() {
        return MycatServer.getInstance().getXATXIDGLOBAL();
    }

    public void setXATXEnabled(boolean xaTXEnabled) {

        if (xaTXEnabled) {
            LOGGER.info("XA Transaction enabled ,con " + this.getSource());
            if (this.xaTXID == null) {
                xaTXID = genXATXID();
            }
        } else {
            LOGGER.info("XA Transaction disabled ,con " + this.getSource());
            this.xaTXID = null;
        }
    }

    public String getXaTXID() {
        return xaTXID;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }


    public boolean isCanClose() {
        return canClose;
    }

    public void setCanClose(boolean canClose) {
        this.canClose = canClose;
    }

    public MiddlerResultHandler getMiddlerResultHandler() {
        return middlerResultHandler;
    }

    public void setMiddlerResultHandler(MiddlerResultHandler middlerResultHandler) {
        this.middlerResultHandler = middlerResultHandler;
    }

    public void setAutoCommitStatus() {
        /* 1.  事务结束后,xa事务结束    */
        if (this.getXaTXID() != null) {
            this.setXATXEnabled(false);
        }
        /* 2. preAcStates 为true,事务结束后,需要设置为true。preAcStates 为ac上一个状态    */
        if (this.getSource().isPreAcStates() && !this.getSource().isAutocommit()) {
            this.getSource().setAutocommit(true);
        }
        this.getSource().clearTxInterrupt();

    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        StringBuilder sb = new StringBuilder();
        for (BackendConnection backCon : target.values()) {
            sb.append(backCon).append("\r\n");
        }
        return sb.toString();
    }
}
