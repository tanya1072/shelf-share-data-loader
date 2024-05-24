package com.example.shelfsharedataloader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;

import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.example.connection.DataStaxAstraProperties;
import com.example.shelfsharedataloader.author.Author;
import com.example.shelfsharedataloader.author.AuthorRepository;
import com.example.shelfsharedataloader.book.Book;
import com.example.shelfsharedataloader.book.BookRepository;

import jakarta.annotation.PostConstruct;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class ShelfShareDataLoaderApplication {

	@Autowired AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;

	@Value("${datadump.location.works}")
	private String worksDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(ShelfShareDataLoaderApplication.class, args);
	}

	private void initAuthors(){
		Path path = Paths.get(authorDumpLocation);
		try(Stream<String> lines = Files.lines(path)){
			lines.forEach(line->{
				//Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try{
					JSONObject jsonObject = new JSONObject(jsonString);
					//Construct author object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));
					//Persist using Repository
					java.lang.System.out.println("Saving author : " + author.getName() + "...");
					authorRepository.save(author);
				}
				catch(JSONException e){
					e.printStackTrace();
				}				
			});
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}

	private void initWorks(){
		Path path = Paths.get(worksDumpLocation);
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines = Files.lines(path)){
			lines.limit(10).forEach(line->{
				//Read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				try{
					JSONObject jsonObject = new JSONObject(jsonString);
					//Construct book object
					Book book = new Book();
					book.setId(jsonObject.getString("key").replace("/works/", ""));
					book.setName(jsonObject.optString("title"));
					JSONObject descriptionObj = jsonObject.optJSONObject("description");
					if(descriptionObj != null){
						book.setDescription(descriptionObj.optString("value"));
					}
					JSONObject publishedObj = jsonObject.optJSONObject("created");
					if(publishedObj != null){
						String dateStr = publishedObj.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
					}
					JSONArray coversJsonArr = jsonObject.optJSONArray("covers");
					if(coversJsonArr != null){
						List<String> coverIds = new ArrayList<>();
						for(int i=0; i<coversJsonArr.length(); i++){
							//coverIds.add(coversJsonArr.getString(i));
							coverIds.add(String.valueOf(coversJsonArr.getInt(i)));
						}
						book.setCoverIds(coverIds);
					}
					JSONArray authorsJsonArr = jsonObject.optJSONArray("authors");
					if(authorsJsonArr != null){
						List<String> authorIds = new ArrayList<>();
						for(int i=0; i<authorsJsonArr.length(); i++){
							String authorId = authorsJsonArr.getJSONObject(i).getJSONObject("author").getString("key")
							.replace("/authors/", "");
							authorIds.add(authorId);
						}
						book.setAuthorIds(authorIds);
						List<String> authorNames = authorIds.stream().map(id -> authorRepository.findById(id))
						.map(optionalAuthor -> {
							if(!optionalAuthor.isPresent()) return "Unknown Author";
							return optionalAuthor.get().getName();
						}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}
					//Persist using Repository
					java.lang.System.out.println("Saving book : " + book.getName() + "...");
					bookRepository.save(book);
				}
				catch(Exception e){
					e.printStackTrace();
				}			
			});
		}
		catch(IOException e){
			e.printStackTrace();
		}
	}

	@PostConstruct
	public void start(){
		//initAuthors();
		initWorks();
	}




	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties) {
			Path bundle = astraProperties.getSecureConnectBundle().toPath();
			return builder -> builder.withCloudSecureConnectBundle(bundle);
	}
}

