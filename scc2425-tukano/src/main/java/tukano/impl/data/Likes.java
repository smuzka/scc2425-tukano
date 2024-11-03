package tukano.impl.data;

import java.util.Objects;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity
public class Likes {

	@Id
	String id;

	String userId;

	String shortId;
	
	public String getOwnerId() {
		return ownerId;
	}

	public void setOwnerId(String ownerId) {
		this.ownerId = ownerId;
	}

	String ownerId;
	
	public Likes() {}

	public Likes(String userId, String shortId, String ownerId) {
		this.id = userId +"+"+shortId;
		this.userId = userId;
		this.shortId = shortId;
		this.ownerId = ownerId;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getShortId() {
		return shortId;
	}

	public void setShortId(String shortId) {
		this.shortId = shortId;
	}

	@Override
	public String toString() {
		return "Likes [userId=" + userId + ", shortId=" + shortId + ", ownerId=" + ownerId + "]";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Likes likes = (Likes) o;
		return Objects.equals(id, likes.id) && Objects.equals(userId, likes.userId) && Objects.equals(shortId, likes.shortId) && Objects.equals(ownerId, likes.ownerId);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, userId, shortId, ownerId);
	}
}
