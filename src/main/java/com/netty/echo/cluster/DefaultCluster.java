package com.netty.echo.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * DefaultCluster
 *
 * <p>Membership is a last-writer-wins map keyed by node id, ordered by
 * incarnation and then by {@link MemberState}. Dead nodes are kept as
 * tombstones for a while so that an older observation cannot bring them back.</p>
 */
public class DefaultCluster implements Cluster {

    private final String name;
    private final Node localNode;
    private final Map<String, Member> nodes;
    private final Map<String, Long> deadSince;
    private long localIncarnation;

    public DefaultCluster(Node localNode) {
        this(UUID.randomUUID().toString(), localNode);
    }

    public DefaultCluster(String name, Node localNode) {
        this(name, localNode, 1);
    }

    public DefaultCluster(String name, Node localNode, long incarnation) {
        this.name = name;
        this.localNode = localNode;
        this.nodes = new LinkedHashMap<>();
        this.deadSince = new HashMap<>();
        this.localIncarnation = incarnation;
    }

    public String getName() {
        return name;
    }

    public Node getMember() {
        return localNode;
    }

    @Override
    public synchronized Node getMember(String id) {
        if (localNode.id().equals(id)) {
            return localNode;
        }

        var member = nodes.get(id);
        return member != null && member.isAlive() ? member.node() : null;
    }

    @Override
    public synchronized Collection<Node> getMembers() {
        return nodes.values().stream()
            .filter(Member::isAlive)
            .map(Member::node)
            .toList();
    }

    @Override
    public synchronized Collection<Member> getMembership() {
        var membership = new ArrayList<Member>(nodes.size() + 1);
        membership.add(getLocalMember());
        membership.addAll(nodes.values());
        return membership;
    }

    @Override
    public synchronized Member getLocalMember() {
        return new Member(localNode, localIncarnation, MemberState.ALIVE);
    }

    @Override
    public synchronized void addMember(Node node) {
        merge(List.of(Member.of(node)));
    }

    @Override
    public synchronized void removeMember(String id) {
        removeMember(id, -1);
    }

    @Override
    public synchronized void removeMember(String id, long incarnation) {
        if (localNode.id().equals(id)) {
            return;
        }

        var existing = nodes.get(id);
        if (existing == null) {
            return;
        }

        var effective = Math.max(incarnation, existing.incarnation());
        merge(List.of(new Member(existing.node(), effective, MemberState.DEAD)));
    }

    @Override
    public synchronized boolean merge(Collection<Member> members) {
        var changed = false;

        for (var candidate : members) {
            if (candidate == null) {
                continue;
            }

            if (candidate.node().id().equals(localNode.id())) {
                changed |= mergeLocal(candidate);
                continue;
            }

            var existing = nodes.get(candidate.node().id());
            if (candidate.supersedes(existing)) {
                nodes.put(candidate.node().id(), candidate);
                if (candidate.isDead()) {
                    deadSince.putIfAbsent(candidate.node().id(), System.currentTimeMillis());
                } else {
                    deadSince.remove(candidate.node().id());
                }
                changed = true;
            }
        }

        return changed;
    }

    /**
     * Handles an observation about the local node. If someone believes we are
     * not alive, bump our incarnation and let the caller gossip the new value.
     */
    private boolean mergeLocal(Member observation) {
        if (observation.incarnation() > localIncarnation) {
            localIncarnation = observation.incarnation();

            if (!observation.isAlive()) {
                localIncarnation++;
                return true;
            }

            return true;
        }

        if (observation.incarnation() == localIncarnation && !observation.isAlive()) {
            localIncarnation++;
            return true;
        }

        return false;
    }

    @Override
    public synchronized void pruneDead(long ttlMillis) {
        var cutoff = System.currentTimeMillis() - ttlMillis;

        var iterator = deadSince.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getValue() <= cutoff) {
                nodes.remove(entry.getKey());
                iterator.remove();
            }
        }
    }
}
