package com.aslan.app;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Hello world!
 *
 */
@SpringBootApplication
public class Application implements CommandLineRunner {

	public static void main(String[] args) {
		System.out.printf("%s\n", "aslan-spring-boot-kafka-example::main(): STARTING THE APPLICATION");
		//SpringApplication.run(Application.class, args);
		
		SpringApplication app = new SpringApplication(Application.class);
		
		String APP_CURRENT_ENV = "local";
		if(System.getenv("work.environment")!=null) {
			String env_value = System.getenv("work.environment").toLowerCase();
			if(env_value.trim().equals("dev")) {
				APP_CURRENT_ENV = "dev";
			}
			
			System.out.printf("%s\n", "aslan-spring-boot-kafka-example::main(): work.environment: " + env_value);
		}
		
		// set active profile
		System.setProperty("spring.profiles.active", APP_CURRENT_ENV);
		
		// current directory
		System.out.printf("%s\n", "aslan-spring-boot-kafka-example::main(): Current Directory= " + System.getProperty("user.dir"));
		
		// Disabling restart: Make this false when you push to cloud (means on cloud), otherwise comment it for local use
		//System.setProperty("spring.devtools.restart.enabled", "false");
				
		
		// run app
		app.run(args);
		System.out.printf("%s\n", "aslan-spring-boot-kafka-example::main(): APPLICATION FINISHED");
		System.out.printf("%s\n", "aslan-spring-boot-kafka-example::main(): env(): " + APP_CURRENT_ENV);
	}
	
	@Override
	public void run(String... args) {
		System.out.printf("%s\n", "aslan-spring-boot-kafka-example::run(): EXECUTING => command line runner");
		try {
			
		} catch (Exception ex) {
			System.out.printf("%s\n", "aslan-spring-boot-kafka-example::run(): Exception: " + ex.getMessage());
			ex.printStackTrace();
		}
	}

} //end class
