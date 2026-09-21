package com.netty.echo.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * DefaultCluster
 */
public class DefaultCluster implements Cluster {

    private final String name;
    private final Node localNode;
    private final Map<String, Node> nodes;

    public DefaultCluster(Node localNode) {
        this(UUID.randomUUID().toString(), localNode);
    }

    public DefaultCluster(String name, Node localNode) {
        this.name = name;
        this.localNode = localNode;
        this.nodes = new LinkedHashMap<>();
    }

    public String getName() {
        return name;
    }

    public Node getMember() {
        return localNode;
    }

    public synchronized Node getMember(String id) {
        if (localNode.id().equals(id)) {
            return localNode;
        }

        return nodes.get(id);
    }

    @Override
    public synchronized void addMember(Node node) {
        if (node == null || node.id().equals(localNode.id())) {
            return;
        }

        nodes.put(node.id(), node);
    }

    @Override
    public synchronized Collection<Node> getMembers() {
        return new ArrayList<>(nodes.values());
    }

    @Override
    public synchronized void removeMember(String id) {
        nodes.remove(id);
    }

    @Override
    public synchronized void setMembers(Collection<Node> members) {
        nodes.clear();
        members.stream()
            .filter(node -> !node.id().equals(localNode.id()))
            .forEach(node -> nodes.put(node.id(), node));
    }
}
