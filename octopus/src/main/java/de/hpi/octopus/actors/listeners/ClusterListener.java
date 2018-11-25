package de.hpi.octopus.actors.listeners;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.actors.Profiler;
import de.hpi.octopus.actors.Reaper;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.Serializable;

public class ClusterListener extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "clusterListener";

	public static Props props() {
		return Props.create(ClusterListener.class);
	}

    ////////////////////
    // Actor Messages //
    ////////////////////

	@Data
	@AllArgsConstructor
    public static class WaitForClusterMessage implements Serializable {
        private static final long serialVersionUID = 4545299111112078209L;

        private int slaves;
        private String csvPath;
    }

	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
	private final Cluster cluster = Cluster.get(this.context().system());

	private int slavesToWaitFor = Integer.MAX_VALUE;
	private int clusterCount = 0;
	private boolean running = false;
	private String csvPath = "";

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberEvent.class, UnreachableMember.class);
		Reaper.watchWithDefaultReaper(this);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////
	
	@Override
	public Receive createReceive() {
		return receiveBuilder().match(CurrentClusterState.class, state -> {
			this.log.info("Current members: {}", state.members());
		}).match(MemberUp.class, mUp -> {
			this.log.info("Member is Up: {}", mUp.member());
			clusterCount++;
			if (!running && clusterCount >= this.slavesToWaitFor && !csvPath.isEmpty()) {
				this.getContext().getSystem().actorSelection("/user/" + Profiler.DEFAULT_NAME)
						.tell(new Profiler.TaskMessage(csvPath), ActorRef.noSender());
            }
		}).match(UnreachableMember.class, mUnreachable -> {
			this.log.info("Member detected as unreachable: {}", mUnreachable.member());
		}).match(MemberRemoved.class, mRemoved -> {
			this.log.info("Member is Removed: {}", mRemoved.member());
            clusterCount--;
        }).match(MemberEvent.class, message -> {
			// ignore
		}).match(Profiler.PoisonPillMessage.class, message -> this.getContext().stop(this.getSelf())
		).match(WaitForClusterMessage.class, message -> {
			csvPath = message.csvPath;
			slavesToWaitFor = message.slaves;

			if (!running && clusterCount >= this.slavesToWaitFor && !csvPath.isEmpty()) {
				this.getContext().getSystem().actorSelection("/user/" + Profiler.DEFAULT_NAME)
						.tell(new Profiler.TaskMessage(csvPath), ActorRef.noSender());
			}
		})
				.build();
	}
}
