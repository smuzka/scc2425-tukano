# Azure TuKano
## Project Assignment #1 - Azure PaaS

* [Assignment Text / Enunciado](scc2425-proj1-tukano)

* [Base Source Code](https://github.com/smduarte/scc2425/tree/main/scc2425-tukano)  - Tukano Maven Project;

## Local WebApp Deployment

In the initial stages of development, it is likely more convenient to deploy your web app to a local instance
of Tomcat10, while using remote Azure Resources (BlobStorage, CosmosDB and Redis Cache).

### Launch local Tomcat10 using Docker

`docker run -ti --net=host smduarte/tomcat10`

**Note:** 

Make sure nothing is already using port 8080 in your host machine.

In Windows and macOS, make sure you have the latest docker version and have `Host Networking` enabled in Settings.

Once Tomcat10 is running, you can access the Tomcat10 manager app via:

[http://127.0.0.1:8080/manager/html](http://tomcat:s3cret@127.0.0.1:8080/manager/html)

If prompted for a login/password, use: `tomcat` and `s3cret`

### Automate deployment with Maven

1. Update your projects' `pom.xml` to include the `tomcat7-maven-plugin` in plugins section:

```xml
<plugin>
   <groupId>org.apache.tomcat.maven</groupId>
   <artifactId>tomcat7-maven-plugin</artifactId>
   <version>2.1</version>
       <configuration>
           <url>http://maven:s3cret@127.0.0.1:8080/manager/text</url>
           <server>tomcat-docker</server>
           <update>true</update>
           <path>/lab1</path>
       </configuration>
</plugin>
```
**Note:** The login is `maven` and `s3cret`and the path must start with `/`
The plugin was originally for Tomcat7 but also works with Tomcat10.

2. To deploy the web application, execute:

   `mvn clean compile package tomcat7:redeploy`

**Note:** Confirm that the .war is being generated in the `target` folder. Make sure
that you have in the pom.xml
```xml
<packaging>war</packaging>
```

3. Try the application. Check the Tomcat10 manager page:

[http://127.0.0.1:8080/manager/html](http://tomcat:s3cret@127.0.0.1:8080/manager/html)


### Code Example 

The project in Lab1 [scc2425-lab1-code.zip](../lab1/scc2425-lab1-code.zip) was updated
to include the local deployment option. Check it out.

### Using Properties to avoid embedding secrets in the source.

Check the updated version of the Lab1 sample code [scc2425-lab1-code.zip](../lab1/scc2425-lab1-code.zip) for an example 
of how to include a properties file in your project.

After loading the properties with `Props.load(propfile)`, you can access the value
with `System.getProperty(name)`.

The updated ControlResource dumps all the properties that are available.

## FAQ

+ **Can I convert the base code to Kotlin?**
  
  Yes. But should you?

+ **Can I convert to Spring and such?**
  
  No. Sorry.

+ **Can I work alone?**
  
  Depends. Explain why, sending an email stating your case.

+ **What is the maximum size of the group?**
  
  Up to 2 Humans. Maximum group grade 40, split among the Human and non-Human (AI) members, according to merit. 
