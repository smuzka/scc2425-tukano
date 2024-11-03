package tukano.api;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.Objects;

@Entity
@Table(name = "AppUser")
public class User {
	
	@Id
	private String id;
	private String pwd;
	private String email;	
	private String displayName;

	public User() {}
	
	public User(String userId, String pwd, String email, String displayName) {
		this.pwd = pwd;
		this.email = email;
		this.id = userId;
		this.displayName = displayName;
	}

	public String getId() {
		return id;
	}
	public void setId(String userId) {
		this.id = userId;
	}
	public String getPwd() {
		return pwd;
	}
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
	public String getEmail() {
		return email;
	}
	public void setEmail(String email) {
		this.email = email;
	}
	public String getDisplayName() {
		return displayName;
	}
	public void setDisplayName(String displayName) {
		this.displayName = displayName;
	}
	
	public String userId() {
		return id;
	}
	
	public String pwd() {
		return pwd;
	}
	
	public String email() {
		return email;
	}
	
	public String displayName() {
		return displayName;
	}
	
	@Override
	public String toString() {
		return "User [userId=" + id + ", pwd=" + pwd + ", email=" + email + ", displayName=" + displayName + "]";
	}
	
	public User copyWithoutPassword() {
		return new User(id, "", email, displayName);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		User user = (User) o;
		return Objects.equals(id, user.id) && Objects.equals(pwd, user.pwd) && Objects.equals(email, user.email) && Objects.equals(displayName, user.displayName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, pwd, email, displayName);
	}

	public User updateFrom(User other ) {
		return new User(id,
				other.pwd != null ? other.pwd : pwd,
				other.email != null ? other.email : email, 
				other.displayName != null ? other.displayName : displayName);
	}
}
