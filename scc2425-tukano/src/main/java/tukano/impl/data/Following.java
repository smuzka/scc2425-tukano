package tukano.impl.data;

import java.util.Objects;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class Following{
  	@Id
  	String id;

	String follower;

	String followee;

	Following() {}

	public Following( String id, String follower, String followee) {
		super();
		this.id = id;
		this.follower = follower;
		this.followee = followee;
	}

	public String getFollower() {
		return follower;
	}

	public void setFollower(String follower) {
		this.follower = follower;
	}

	public String getFollowee() {
		return followee;
	}

	public void setFollowee(String followee) {
		this.followee = followee;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Following following = (Following) o;
		return Objects.equals(id, following.id) && Objects.equals(follower, following.follower) && Objects.equals(followee, following.followee);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, follower, followee);
	}

	@Override
	public String toString() {
		return "Following [follower=" + follower + ", followee=" + followee + "]";
	}
	
	
}