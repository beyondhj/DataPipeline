/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.github.datapipeline.core.config;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import org.apache.commons.collections.MapUtils;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.traverse.LexBreadthFirstIterator;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The configuration of job config
 */
public class JobConfig {

    public static JobConfig parse(String json) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();s
        Map<String, Object> map = mapper.readValue(json, Map.class);
        JobConfig jobConfig = new JobConfig();
        jobConfig.init(map);
        return jobConfig;
    }

    private String jobType;

    private String version;

    private SettingConfig settingConfig;

    private DirectedAcyclicGraph<NodeData, DefaultEdge> graph = new DirectedAcyclicGraph<NodeData, DefaultEdge>(
            DefaultEdge.class);

    private List<NodeData> graphNodes = Lists.newArrayList();

    public String getJobType() {
        return jobType;
    }

    public String getVersion() {
        return version;
    }

    public SettingConfig getSettingConfig() {
        return settingConfig;
    }

    public List<NodeData> getGraphNodes() {
        return graphNodes;
    }

    private void init(Map<String, Object> map) {
        this.version = MapUtils.getString(map, "version");
        this.settingConfig = new SettingConfig((Map<String, Object>) MapUtils.getObject(map, "setting"));
        List<Map<String, Object>> nodes = (List<Map<String, Object>>) MapUtils.getObject(map, "nodes");
        Map<String, NodeData> nodeDatas = addGraphNodes(nodes);
        List<Map<String, Object>> edges = (List<Map<String, Object>>) MapUtils.getObject(map, "edges");
        addGraphEdges(nodeDatas, edges);
        initGraphNodes();
    }


    private Map<String, NodeData> addGraphNodes(List<Map<String, Object>> nodes) {
        Map<String, NodeData> nodeDatas = Maps.newHashMap();
        for (Map<String, Object> node : nodes) {
            NodeData nodeData = addGraphNode(node);
            nodeDatas.put(nodeData.getId(), nodeData);
        }
        return nodeDatas;
    }

    private NodeData addGraphNode(Map<String, Object> node) {
        String id = MapUtils.getString(node, "id");
        String name = MapUtils.getString(node, "name");
        String type = MapUtils.getString(node, "type");
        NodeConfig nodeConfig = new NodeConfig((Map<String, Object>) MapUtils.getObject(node, "config"));
        NodeData nodeData = new NodeData();
        nodeData.setId(id);
        nodeData.setName(name);
        nodeData.setType(type);
        nodeData.setConfig(nodeConfig);
        this.graph.addVertex(nodeData);
        return nodeData;
    }

    private void addGraphEdges(Map<String, NodeData> nodeDatas, List<Map<String, Object>> edges) {
        for (Map<String, Object> edge : edges) {
            addGraphEdge(nodeDatas, edge);
        }
    }

    private void addGraphEdge(Map<String, NodeData> nodeDatas, Map<String, Object> edge) {
        String source = MapUtils.getString(edge, "source");
        String target = MapUtils.getString(edge, "target");
        NodeData sourceVertex = nodeDatas.get(source);
        NodeData targetVertex = nodeDatas.get(target);
        this.graph.addEdge(sourceVertex, targetVertex);
    }

    private void initGraphNodes() {
        LexBreadthFirstIterator<NodeData, DefaultEdge> breadthFirstIterator = new LexBreadthFirstIterator(graph);
        while (breadthFirstIterator.hasNext()) {
            NodeData next = breadthFirstIterator.next();
            graphNodes.add(next);
        }
    }

    public NodeData getGraphAncestor(NodeData vertex) {
        Set<NodeData> ancestors = graph.getAncestors(vertex);
        Iterator<NodeData> iterator = ancestors.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        throw new IllegalStateException();
    }
}
