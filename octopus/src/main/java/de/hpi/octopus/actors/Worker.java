package de.hpi.octopus.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.hpi.octopus.OctopusMaster;
import de.hpi.octopus.actors.Profiler.CompletionMessage;
import de.hpi.octopus.actors.Profiler.RegistrationMessage;
import lombok.AllArgsConstructor;
import lombok.Data;

public class Worker extends AbstractActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	////////////////////
	// Actor Messages //
	////////////////////
	
	@SuppressWarnings("unused")
	public abstract static class WorkMessage implements Serializable {
		protected static final long serialVersionUID = -7643194361868862395L;
		private WorkMessage() {}
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessagePasswordCracking extends WorkMessage {
		private static final long serialVersionUID = -7643194369068862395L;
		private WorkMessagePasswordCracking() {}
		private String[] sixDigitNumbers;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessageLinearCombination extends WorkMessage {
		private static final long serialVersionUID = -7643194361869062395L;
		private WorkMessageLinearCombination() {}
		private int[] numbers;
		private long start;
		private long end;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessageGeneComparision extends WorkMessage {
		private static final long serialVersionUID = -7643190061869062395L;
		private WorkMessageGeneComparision() {}
		private String first;
		private String second;
		private int firstID;
		private int secondID;
	}

	@Data @AllArgsConstructor @SuppressWarnings("unused")
	public static class WorkMessageFindHash extends WorkMessage {
		private static final long serialVersionUID = -7643190061869062315L;
		private WorkMessageFindHash() {}
		private int id;
		private String prefix;
		private int content;
	}

	/////////////////
	// Actor State //
	/////////////////
	
	private final LoggingAdapter log = Logging.getLogger(this.context().system(), this);
	private final Cluster cluster = Cluster.get(this.context().system());

	/////////////////////
	// Actor Lifecycle //
	/////////////////////
	
	@Override
	public void preStart() {
		this.cluster.subscribe(this.self(), MemberUp.class);
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
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(WorkMessagePasswordCracking.class, this::handle)
				.match(WorkMessageLinearCombination.class, this::handle)
				.match(WorkMessageGeneComparision.class, this::handle)
				.match(WorkMessageFindHash.class, this::handle)
				.match(Profiler.PoisonPillMessage.class, message -> this.getContext().stop(this.getSelf()))
				.matchAny(object -> this.log.info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if (member.hasRole(OctopusMaster.MASTER_ROLE))
			this.getContext()
				.actorSelection(member.address() + "/user/" + Profiler.DEFAULT_NAME)
				.tell(new RegistrationMessage(), this.self());
	}

	private void handle(WorkMessagePasswordCracking message) {
		/*
		String[] numbers = new String[2];
		numbers[0] = "000900";
		numbers[1] = "000001";
		this.log.info("test: " + this.calculateHashes(numbers)[0] + " ###AND### " + this.calculateHashes(numbers)[1]);*/

		this.log.info("Recieved work package. Smallest password: " + message.sixDigitNumbers[0]);
		String[] hashes = this.calculateHashes(message.sixDigitNumbers);

		this.sender().tell(new Profiler.CompletionMessagePasswordCracking(CompletionMessage.status.SUCCESS, hashes), this.self());
	}

	private void handle(WorkMessageLinearCombination message) {
		/*
		String[] numbers = new String[2];
		numbers[0] = "000000";
		numbers[1] = "000004";
		this.log.info("test: " + this.calculateHashes(numbers)[0] + " ###AND### " + this.calculateHashes(numbers)[1]);
		*/

		this.log.info("Recieved work package. Smallest index: " + Long.toString(message.start, 2) + " - " + Long.toString(message.end, 2));

		for (long a = message.start; a < message.end; a++) {
			String binary = Long.toBinaryString(a);

			int[] prefixes = new int[message.numbers.length];
			for (int i = 0; i < prefixes.length; i++)
				prefixes[i] = 1;

			int i = 0;
			for (int j = binary.length() - 1; j >= 0; j--) {
				if (binary.charAt(j) == '1')
					prefixes[i] = -1;
				i++;
			}

			if (this.sum(message.numbers, prefixes) == 0) {
				this.sender().tell(new Profiler.CompletionMessageLinearCombination(Profiler.CompletionMessageLinearCombination.status.SUCCESS, true, prefixes), this.self());
				return;
			}
		}
		this.sender().tell(new Profiler.CompletionMessageLinearCombination(Profiler.CompletionMessageLinearCombination.status.SUCCESS, false, new int[]{}), this.self());
	}

	private int sum(int[] numbers, int[] prefixes) {
		int sum = 0;
		for (int i = 0; i < numbers.length; i++)
			sum += numbers[i] * prefixes[i];
		return sum;
	}

	private void handle(WorkMessageGeneComparision message) {
		this.log.info("Recieved work package for gene comparision.");

		int length = this.longestOverlap(message.first, message.second).length();

		this.sender().tell(new Profiler.CompletionMessageGeneComparsion(Profiler.CompletionMessageGeneComparsion.status.SUCCESS, length), this.self());
	}

	private void handle(WorkMessageFindHash message) {
		this.log.info("Received work package for hash search.");
		final int prefixLength = 5;
		final String prefix = message.prefix;
		final String content = Integer.toString(message.content);

		StringBuilder fullPrefixBuilder = new StringBuilder();
		for (int i = 0; i < prefixLength; i++)
			fullPrefixBuilder.append(prefix);

		Random rand = new Random(13);

		String fullPrefix = fullPrefixBuilder.toString();
		int nonce = 0;
		while (true) {
			nonce = rand.nextInt();
			String hash = this.calulateHash(content + nonce);
			if (hash.startsWith(fullPrefix)) {
				this.sender().tell(new Profiler.CompletionMessageFindHash(Profiler.CompletionMessageFindHash.status.SUCCESS, hash), this.self());
				break;
			}
		}
	}

	private String longestOverlap(String str1, String str2) {
		if (str1.isEmpty() || str2.isEmpty())
			return "";

		if (str1.length() > str2.length()) {
			String temp = str1;
			str1 = str2;
			str2 = temp;
		}

		int[] currentRow = new int[str1.length()];
		int[] lastRow = str2.length() > 1 ? new int[str1.length()] : null;
		int longestSubstringLength = 0;
		int longestSubstringStart = 0;

		for (int str2Index = 0; str2Index < str2.length(); str2Index++) {
			char str2Char = str2.charAt(str2Index);
			for (int str1Index = 0; str1Index < str1.length(); str1Index++) {
				int newLength;
				if (str1.charAt(str1Index) == str2Char) {
					newLength = str1Index == 0 || str2Index == 0 ? 1 : lastRow[str1Index - 1] + 1;

					if (newLength > longestSubstringLength) {
						longestSubstringLength = newLength;
						longestSubstringStart = str1Index - (newLength - 1);
					}
				} else {
					newLength = 0;
				}
				currentRow[str1Index] = newLength;
			}
			int[] temp = currentRow;
			currentRow = lastRow;
			lastRow = temp;
		}
		return str1.substring(longestSubstringStart, longestSubstringStart + longestSubstringLength);
	}

	private String calulateHash(String password) {
		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		byte[] hashedBytes = new byte[0];
		try {
			hashedBytes = digest.digest(password.getBytes("UTF-8"));
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < hashedBytes.length; i++){
			stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
		}
		return stringBuffer.toString();
	}

	private String[] calculateHashes(String[] sixDigitNumbers){
		String[] calculatedHashes = new String[sixDigitNumbers.length];
		for (int i = 0; i < sixDigitNumbers.length; i++){
			calculatedHashes[i] = this.calulateHash(sixDigitNumbers[i]);
		}
		return calculatedHashes;
	}

	private List<List<Integer>> calculateNewRow (List<List<Integer>> previousRow, int password, int startIndex, int endIndex){
		List<List<Integer>> currentRow = new ArrayList<>(Collections.nCopies(endIndex-startIndex, null));
		for(int i = startIndex; i < endIndex; i++){
			if(previousRow.get(i) == null){
				if( i-password >= 0 && previousRow.get(i-password) != null){
					List<Integer> temp = new ArrayList<>(previousRow.get(i-password));
					temp.add(password);
					currentRow.set(i-startIndex, temp);
				}
			}
			else{
				currentRow.set(i-startIndex, previousRow.get(i));
			}
		}
		return currentRow;
	}
}