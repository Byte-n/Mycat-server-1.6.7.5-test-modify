package io.mycat.config.loader.xml;

import io.mycat.backend.mysql.nio.MySQLConnection;
import io.mycat.backend.mysql.nio.handler.MultiNodeQueryHandler;
import io.mycat.config.util.ConfigUtil;
import io.mycat.route.RouteResultsetNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

public class XMLStreamingLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(XMLStreamingLoader.class);
    /**
     * 默认的dtd文件路径
     */
    private final static String DEFAULT_DTD = "/streaming.dtd";
    /**
     * 默认的xml文件路径
     */
    private final static String DEFAULT_XML = "/streaming.xml";
    /**
     * 根节点
     */
    private RootNode rootNode;

    public XMLStreamingLoader(String streamingXMLFileUrl) {
        this.load(streamingXMLFileUrl);
    }

    public XMLStreamingLoader() {
        this(DEFAULT_XML);
    }


    /**
     * 判断当前MySQLConnection连接是否开启流式
     *
     * @param msc MySQLConnection
     * @return
     */
    public boolean isStreaming(MySQLConnection msc) {
        if (msc.getRespHandler() instanceof MultiNodeQueryHandler) {
            //如果需要拿到全部的结果集再处理，再发送给客户端，则是通过MultiNodeQueryHandler.dataMergeSvr处理的。不需要，则此对象为null
            if (((MultiNodeQueryHandler) msc.getRespHandler()).getDataMergeSvr() != null) {
                LOGGER.warn("多节点查询不支持:需要拿取到所有数据后，再做处理，再发送给客户端的查询,例如：order by、sort、limit等,流式查询判定：false");
                return false;
            }
        }
        return this.rootNode.check(msc);
    }

    /**
     * 加载配置文件
     *
     * @param xmlFile
     */
    private void load(String xmlFile) {
        InputStream dtd = XMLSchemaLoader.class.getResourceAsStream(XMLStreamingLoader.DEFAULT_DTD);
        InputStream xml = XMLSchemaLoader.class.getResourceAsStream(xmlFile);
        try {
            //获取根节点
            Element root = ConfigUtil.getDocument(dtd, xml).getDocumentElement();
            String rootTagName = root.getTagName();
            //全局流式。
            String isGlobalStreaming = root.getAttribute("streaming");
            String sqlHitHistNum = root.getAttribute("sql-hit-hist-num");
            int num = -1;
            try {
                num = Integer.parseInt(sqlHitHistNum);
            } catch (NumberFormatException e) {
                LOGGER.error("streaming.xml文件的mycat:streaming标签的sql-hit-hist-num属性，输入不合法：" + sqlHitHistNum + "（当前重置为:" + num + "）   errorMsg: " + e.getMessage());
            }
            //创建根节点.（因为dtd文件做了约束这里不判断也行）
            rootNode = new RootNode(rootTagName, Boolean.valueOf(isGlobalStreaming), num);

            //备注:加载顺序:dataNode->table->sql  ，这样的顺序方便标签加载时的去重
            //加载节点
            loadDataNodes(root, this.rootNode.dataNodes);
            //加载表格
            loadTableNodes(root, this.rootNode.tables);
            //加载sql
            loadSQLNodes(root, this.rootNode.sqls);

            //清理资源
            this.rootNode.r_sqls = null;
            this.rootNode.r_tables = null;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (dtd != null) {
                    dtd.close();
                }
                if (xml != null) {
                    xml.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 加载sql标签
     *
     * @param parent 父节点
     * @param sqls   存放对象的集合
     */
    private void loadSQLNodes(Element parent, LinkedHashSet<SQLNode> sqls) {
        //从parent下获取所有的sql标签
        NodeList sqlNodes = parent.getElementsByTagName("sql");

        Element s;//节点
        String content;//内容，标签对里面的内容
        String mode;
        //遍历
        for (int i = 0; i < sqlNodes.getLength(); i++) {
            s = (Element) sqlNodes.item(i);
            content = s.getTextContent().trim();
            //去重检测
            if (this.rootNode.r_sqls.containsKey(content)) {
                continue;
            }
            //如果不重复，就条件到重复判断集合中
            this.rootNode.r_sqls.put(content, null);
            //标签对内容不为空
            if (!content.isEmpty()) {
                mode = s.getAttribute("mode");
                sqls.add(new SQLNode(content, mode, content));
            }
        }

    }

    /**
     * 加载table标签
     *
     * @param parent 父节点
     * @param tables 存放对象的集合
     */
    private void loadTableNodes(Element parent, Map<String, TableNode> tables) {
        //从parent下获取所有的table标签
        NodeList tableNodes = parent.getElementsByTagName("table");
        Element table;
        String name;
        TableNode tableNode;
        String mode;
        //遍历
        for (int i = 0; i < tableNodes.getLength(); i++) {
            table = (Element) tableNodes.item(i);
            //先获取name属性
            name = TableNode.handleStr(table.getAttribute("name"));
            //去重检测
            if (this.rootNode.r_tables.containsKey(name)) {
                continue;
            }
            //如果不重复，就条件到重复判断集合中
            this.rootNode.r_tables.put(name, null);
            //获取mode属性
            mode = table.getAttribute("mode");
            //创建表格节点
            tableNode = new TableNode(name, mode);
            //处理表格下的sql节点
            this.loadSQLNodes(table, tableNode.sqls);
            //添加到集合
            tables.put(name, tableNode);
        }
    }

    /**
     * 加载table标签
     *
     * @param root      根节点。因为dataNode只会存在于根节点下
     * @param dataNodes 存放对象的集合
     */
    private void loadDataNodes(Element root, Map<String, DataNode> dataNodes) {
        //从root下获取所有的dataNode
        NodeList dataNodeNodes = root.getElementsByTagName("dataNode");
        Element dn;
        String name;
        DataNode dataNode;
        String mode;
        //遍历dataNodeNodes
        for (int i = 0; i < dataNodeNodes.getLength(); i++) {
            dn = (Element) dataNodeNodes.item(i);
            //获取属性
            name = DataNode.handleStr(dn.getAttribute("name"));
            mode = dn.getAttribute("mode");
            //创建实例
            dataNode = new DataNode(name, mode);
            //加载此节点下的表格
            this.loadTableNodes(dn, dataNode.tables);
            //加载此节点下的sql    必须先加载table。因为table下面也可能有sql节点
            this.loadSQLNodes(dn, dataNode.sqls);
            //加入到集合
            dataNodes.put(name, dataNode);
        }
    }

    /**
     * 节点   抽象
     */
    protected abstract static class Node {
        /**
         * include mode
         */
        protected final static String MODE_INCLUDE = "include";
        /**
         * except mode  虽然没用...留个标记
         */
        protected final static String MODE_EXCEPT = "except";
        /**
         * 当前节点处理的模式
         * 默认MODE_INCLUDE。XML文件里面mode属性也是默认MODE_INCLUDE
         */
        protected String mode = MODE_INCLUDE;
        /**
         * 当前节点的名称
         */
        protected String name;

        private Node() {
        }

        /**
         * 检查MySQLConnection是否开启流式
         *
         * @return true：流式
         */
        protected abstract Boolean check(MySQLConnection msc);


    }

    /**
     * 根节点：
     * &lt;mycat:streaming&gt;&lt;/mycat:streaming&gt;
     */
    public static class RootNode extends Node {
        /**
         * 全局SQL：
         * &lt;mycat:streaming&gt;
         * &lt;sql&gt;&lt;/sql&gt;
         * &lt;/mycat:streaming&gt;
         */
        private final LinkedHashSet<SQLNode> sqls;
        /**
         * sql判定，命中历史
         */
        private ConcurrentHashMap<String, Boolean> sqlHitHistory;
        /**
         * 如果sql正则判定命中的，将判定命中的sql语句存入此有序数组（当从sqlHitHist移除最后一个时，会使用到。因为从sqlHitHist中移除最后一个，实际上是移除最先存入的那个结果,也就是此数组的第一个）
         */
        private CopyOnWriteArrayList<String> sqlHitHistoryName;
        /**
         * 全局表格:
         * &lt;mycat:streaming&gt;
         * &lt;table&gt;&lt;/table&gt;
         * &lt;/mycat:streaming&gt;
         */
        private final Map<String, TableNode> tables;
        /**
         * 节点：
         * &lt;mycat:streaming&gt;
         * &lt;dataNode&gt;&lt;/dataNode&gt;
         * &lt;/mycat:streaming&gt;
         */
        private final Map<String, DataNode> dataNodes;
        /**
         * 标记全局流式
         */
        private final Boolean isGlobalStreaming;
        private static int sqlHitHistNum;

        //用来记录以及解析过的标签节点。因为getElementsByTagName会获取所有子代节点，所以子开始向上解析，并记录排除重复的，就可以避免。
        private Map<String, Object> r_sqls;
        private Map<String, Object> r_tables;

        protected RootNode(String name, Boolean isGlobalStreaming, int num) {
            this.mode = null;
            this.name = name;
            this.isGlobalStreaming = isGlobalStreaming;
            sqls = new LinkedHashSet<>();
            tables = new HashMap<>();
            dataNodes = new HashMap<>();
            r_sqls = new HashMap<>();
            r_tables = new HashMap<>();
            sqlHitHistNum = num;
            if (sqlHitHistNum > 0) {
                sqlHitHistory = new ConcurrentHashMap<>();
                sqlHitHistoryName = new CopyOnWriteArrayList<>();
                LOGGER.info("sql标签的命中缓存数：" + sqlHitHistNum);
            } else {
                LOGGER.info("不启用sql标签的命中缓存:" + sqlHitHistNum);
            }
        }

        @Override
        protected Boolean check(MySQLConnection msc) {
            //无法通过attachment判断。
            if (!(msc.getAttachment() instanceof RouteResultsetNode)) {
                return false;
            }
            //全局是否开启
            boolean b = this.isGlobalStreaming;
            LOGGER.debug("isGlobalStreaming：" + isGlobalStreaming);
            //路由结果对象
            RouteResultsetNode attachment = (RouteResultsetNode) msc.getAttachment();
            long id = msc.getId();//ID
            String n = attachment.getName();//节点名称
            List<String> ts = attachment.getSource().getTables();//表格名称
            String statement = attachment.getStatement();//SQL语句
			/*
			sql判断
			 */
            //获取MySQLConnection查询的SQL
            Boolean sqlCheck = SQLNode.check(this.sqls, statement, this.sqlHitHistory, this.sqlHitHistoryName, this.getClass());
            if (sqlCheck != null) {
                b = sqlCheck;
            }

            if (ts==null){
                return false;
            }
			/*
			表格判断
			 */
            //获取当前MySQLConnection查询包含得所有表格
            if (ts.size() > 1) {
                LOGGER.warn("不支持多表:" + id + " |-> DataNode:" + n + "    tables:" + ts + "    sql:" + statement + "    streaming:" + b);
                return false;
            }
            //只支持单表
            if (ts.size() == 1) {
                TableNode tableNode = this.tables.get(TableNode.handleStr(ts.get(0)));
                if (tableNode != null) {
                    Boolean check = tableNode.check(attachment);
                    //再判断一次
                    if (check != null) {
                        b = check;
                        LOGGER.debug(this.name + "节点下的table节点判定结果：" + tableNode + ",result:" + b);
                    }
                }
            }

			/*
			最后节点
			 */
            //根据MySQLConnection查询对应的节点，尝试从dataNodes中获取
            DataNode dataNode = this.dataNodes.get(DataNode.handleStr(n));
            //不为空，则处理
            if (dataNode != null) {
                b = dataNode.check(attachment);
                LOGGER.debug("dataNode节点判定结果：" + dataNode + ",result:" + b);
            }
            //日志打印
            LOGGER.info(id + " |-> DataNode:" + n + "    tables:" + ts + "    sql:" + statement + "    streaming:" + b);
            //返回最中的判断结果：局部覆盖全局，子覆盖父
            return b;
        }

        @Override
        public String toString() {
            return "RootNode{" +
                    "mode='" + mode + '\'' +
                    ", name='" + name + '\'' +
                    ", sqls.size()=" + sqls.size() +
                    ", sqlHitHistory.size()=" + sqlHitHistory.size() +
                    ", tables.size()=" + tables.size() +
                    ", dataNodes.size()=" + dataNodes.size() +
                    ", isGlobalStreaming=" + isGlobalStreaming +
                    '}';
        }
    }

    /**
     * sql节点： name和Pattern对应的正则表达式一致
     * <p>
     * &lt;sql&gt;&lt;/sql&gt;
     */
    protected static class SQLNode extends Node {
        /**
         * SQL语句：标签对内的内容（去除空格，换行符，转小写）
         */
        protected final Pattern regex;

        protected SQLNode(String name, String mode, String regex) {
            this.name = name;
            this.regex = Pattern.compile(regex);
            this.mode = mode;
        }

        /**
         * 请调用public static boolean check(LinkedHashSet<SQLNode> sqls, String sql)方法！
         *
         * @param msc ‘
         * @return ’
         */
        @Override
        @Deprecated
        protected Boolean check(MySQLConnection msc) {
            return null;
        }

        /**
         * 是否开启流式
         *
         * @param sql ’
         * @return true:开启流式、false：不开启流式、null:正则不匹配
         */
        @Nullable
        private Boolean check(@NotNull String sql) {
            boolean matches = this.regex.matcher(sql).matches();
            if (matches) {
                return this.mode.equals(MODE_INCLUDE);
            }
            return null;
        }

        /**
         * 用sqls中的正则去匹配sql,并返回匹配结果，如果没有任何正则命中，则返回null
         *
         * @param sqls          sql标签节点
         * @param sql           要判定的sql
         * @param sqlHitHist    sql命中缓存的集合
         * @param sqlHitHisName 如果sql正则判定命中的，将判定命中的sql语句存入此有序数组（当从sqlHitHist移除最后一个时，会使用到、因为从sqlHitHist最后一个，实际上是移除最先存入的那个结果,也就是此数组的第一个）
         * @param c             Class对象，用来判断控制dedug日志信息前缀内容
         * @return ’
         */
        public static Boolean check(@NotNull LinkedHashSet<SQLNode> sqls, String sql, ConcurrentHashMap<String, Boolean> sqlHitHist, CopyOnWriteArrayList<String> sqlHitHisName, Class<?> c) {
            Boolean is = null;
            SQLNode sqlNode = null;
            Boolean check;

            //日志信息前缀处理
            String msgPrefix = "";
            if (RootNode.class.equals(c)) {
                msgPrefix = "根节点下：";
            }
            if (TableNode.class.equals(c)) {
                msgPrefix = "table节点下：";
            }
            if (DataNode.class.equals(c)) {
                msgPrefix = "dataNode节点下：";
            }


            //尝试从历史中获取判定
            if (sqlHitHist != null) {
                Boolean b_ = sqlHitHist.get(sql);
                if (b_ != null) {
                    LOGGER.info(msgPrefix + "命中sql正则命中缓存：{sql:" + sql + "，resule:" + b_ + "}");
                    return b_;
                }
            }

            //循环，匹配正则，如果有多个，则最后一个为准。
            for (SQLNode sn : sqls) {
                check = sn.check(sql);
                if (check != null) {
                    is = check;
                    sqlNode = sn;
                    LOGGER.debug(msgPrefix + "sql正则命中：{sql:" + sqlNode + "，resule:" + is + "}");
                }
            }

            if (sqlNode != null && sqlHitHist != null) {
                //保证集合的大小,移除最后一个
                if (sqlHitHist.size() >= RootNode.sqlHitHistNum) {
                    //从顺序数组中，获取最开启存入的那一个
                    String k = sqlHitHisName.remove(0);
                    Boolean remove = sqlHitHist.remove(k);
                    LOGGER.debug(msgPrefix + "从sql正则命中缓存移除：{sql:" + k + "，resule:" + remove + "}");
                }
                //加入缓存
                sqlHitHist.put(sql, is);
                sqlHitHisName.add(sql);
                LOGGER.debug(msgPrefix + "加入/更新sql正则命中缓存：{sql:" + sql + "，resule:" + is + "},当前已缓存数：" + sqlHitHist.size());
            }

            return is;
        }

        @Override
        public String toString() {
            return "SQLNode{" +
                    "mode='" + mode + '\'' +
                    ", name='" + name + '\'' +
                    ", regex=" + regex +
                    '}';
        }
    }

    /**
     * 表格节点:
     * &lt;table&gt;&lt;/table&gt;
     */
    protected static class TableNode extends Node {
        /**
         * 表格内的sql节点
         */
        protected final LinkedHashSet<SQLNode> sqls = new LinkedHashSet<>();
        /**
         * sql判定，命中历史
         */
        private ConcurrentHashMap<String, Boolean> sqlHitHistory;
        /**
         * 如果sql正则判定命中的，将判定命中的sql语句存入此有序数组（当从sqlHitHist移除最后一个时，会使用到。因为从sqlHitHist中移除最后一个，实际上是移除最先存入的那个结果,也就是此数组的第一个）
         */
        private CopyOnWriteArrayList<String> sqlHitHistoryName;

        protected TableNode(String name, String mode) {
            if (RootNode.sqlHitHistNum > 0) {
                sqlHitHistory = new ConcurrentHashMap<>();
                sqlHitHistoryName = new CopyOnWriteArrayList<>();
            }
            this.name = handleStr(name);
            this.mode = mode;
        }

        /**
         * 字符串处理：转小写
         *
         * @param str 要处理的字符串
         * @return 处理后的字符串
         */
        public static String handleStr(String str) {
            return str.toLowerCase();
        }

        @Override
        protected Boolean check(MySQLConnection msc) {
            //无法通过attachment判断
            if (!(msc.getAttachment() instanceof RouteResultsetNode)) {
                return false;
            }
            RouteResultsetNode attachment = (RouteResultsetNode) msc.getAttachment();
            return check(attachment);
        }

        protected Boolean check(RouteResultsetNode attachment) {
            //无法通过attachment判断
            if (attachment == null) {
                return false;
            }
            //表格判断
            List<String> tables = attachment.getSource().getTables();
            //没有表格就不需要判断
            if (tables.size() == 0) {
                return null;
            }
            //不支持多表
            if (tables.size() >= 2) {
                return false;
            }
            //初始判断值。没有表格,是包含=false/是排除=true
            boolean is = !this.mode.equals(MODE_INCLUDE);
            String table = tables.get(0);
            //格判断处理
            if (this.name.equals(TableNode.handleStr(table))) {
                is = this.check(table);
            }

            //表格下的SQL判断
            Boolean sqlCheck = SQLNode.check(this.sqls, attachment.getStatement(), this.sqlHitHistory, this.sqlHitHistoryName, this.getClass());
            if (sqlCheck != null) {
                is = sqlCheck;
            }
            //否则返回sql的判断结果
            return is;
        }

        private boolean check(String table) {
            if (this.mode.equals(MODE_INCLUDE)) {//包含
                return this.name.equals(handleStr(table));
            } else {
                return !this.name.equals(handleStr(table));
            }
        }

        @Override
        public String toString() {
            return "TableNode{" +
                    "mode='" + mode + '\'' +
                    ", name='" + name + '\'' +
                    ", sqls.size()=" + sqls.size() +
                    ", sqlHitHistory.size()=" + sqlHitHistory.size() +
                    '}';
        }
    }

    /**
     * 节点：
     * &lt;dataNode&gt;
     * &lt;/dataNode&gt;
     */
    protected static class DataNode extends Node {
        /**
         * 节点下的sql
         */
        protected final LinkedHashSet<SQLNode> sqls = new LinkedHashSet<>();
        /**
         * 节点下的表格
         */
        protected final Map<String, TableNode> tables = new HashMap<>();

        /**
         * sql判定，命中历史
         */
        private ConcurrentHashMap<String, Boolean> sqlHitHistory;
        /**
         * 如果sql正则判定命中的，将判定命中的sql语句存入此有序数组（当从sqlHitHist移除最后一个时，会使用到。因为从sqlHitHist中移除最后一个，实际上是移除最先存入的那个结果,也就是此数组的第一个）
         */
        private CopyOnWriteArrayList<String> sqlHitHistoryName;

        protected DataNode(String name, String mode) {
            if (RootNode.sqlHitHistNum > 0) {
                sqlHitHistory = new ConcurrentHashMap<>();
                sqlHitHistoryName = new CopyOnWriteArrayList<>();
            }
            this.name = handleStr(name);
            this.mode = mode;
        }

        /**
         * 字符串处理：转小写
         *
         * @param str 要处理的字符串
         * @return 处理后的字符串
         */
        private static String handleStr(String str) {
            return str.toLowerCase();
        }

        @Override
        protected Boolean check(MySQLConnection msc) {
            if (!(msc.getAttachment() instanceof RouteResultsetNode)) {
                return false;
            }
            RouteResultsetNode attachment = (RouteResultsetNode) msc.getAttachment();
            return check(attachment);
        }

        protected boolean check(RouteResultsetNode attachment) {
            if (attachment == null) {
                return false;
            }
            boolean is = check(attachment.getName());

            //表格判断
            List<String> ts = attachment.getSource().getTables();
            //只支持单表
            if (ts.size() == 1) {
                TableNode tableNode = this.tables.get(TableNode.handleStr(ts.get(0)));
                if (tableNode != null) {
                    Boolean check = tableNode.check(attachment);
                    //再判断一次
                    if (check != null) {
                        is = check;
                    }
                }
            }

            //SQL判断
            Boolean sqlCheck = SQLNode.check(this.sqls, attachment.getStatement(), this.sqlHitHistory, this.sqlHitHistoryName, this.getClass());
            if (sqlCheck != null) {
                is = sqlCheck;
            }
            return is;
        }

        private boolean check(String nodeName) {
            if (this.mode.equals(MODE_INCLUDE)) {//包含
                return this.name.equals(handleStr(nodeName));
            } else {
                return !this.name.equals(handleStr(nodeName));
            }
        }

        @Override
        public String toString() {
            return "DataNode{" +
                    "mode='" + mode + '\'' +
                    ", name='" + name + '\'' +
                    ", sqls.size()=" + sqls.size() +
                    ", tables.size()=" + tables.size() +
                    ", sqlHitHistory.size()=" + sqlHitHistory.size() +
                    '}';
        }
    }
}
