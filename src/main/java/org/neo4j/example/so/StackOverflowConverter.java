package org.neo4j.example.so;

import au.com.bytecode.opencsv.CSVWriter;

import javax.xml.crypto.dsig.XMLObject;
import javax.xml.stream.XMLStreamReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author mh
 * @since 13.10.15
 */
public class StackOverflowConverter {

    enum Files {
// TODO enable if needed
//        Badges(),
//        Comments(),
//        PostHistory(),
//        PostLinks(),
//        Votes(),

        Posts("Id#id:ID(Post)", "Title#title", "PostTypeId#postType:INT", "CreationDate#createdAt:datetime", "Score#score:INT", "ViewCount#views:INT", "AnswerCount#answers:INT", "CommentCount#comments:INT", "FavoriteCount#favorites:INT", "LastEditDate#updatedAt:datetime") { // ,"Body#body"
            { callback = new PostsProcessCallback(); }
        },
        Tags("TagName#name:ID(Tag)", "Count#count:INT", "WikiPostId#wikiPostId:INT"),
        Users("Id#id:ID(User)", "DisplayName#name", "Reputation#reputation:INT", "CreationDate#createdAt:datetime", "LastAccessDate#accessedAt:datetime", "WebsiteUrl#url", "Location#location", "Views#views:INT", "UpVotes#upvotes:INT", "DownVotes#downvotes:INT", "Age#age:INT", "AccountId#accountId:INT");

        String[] columns;
        ProcessCallback callback = ProcessCallback.NONE;

        Files(String... columns) {
            this.columns = columns;
        }

        public static Files matches(String fileName) {
            for (Files file : values()) {
                if (fileName.matches(file.name() + "(?i)\\.xml(\\.gz)?")) return file;
            }
            return null;
        }

    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 1 || !new File(args[0]).isDirectory())
            System.err.println("Usage StackOverflowConverter source-dir\nThe source dir should hold [gzipped] xml files of a StackExchange dump");
        File[] filesInDirectory = new File(args[0]).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return Files.matches(name) != null;
            }
        });
        String[] arguments = new String[filesInDirectory.length];
        for (int i = 0; i < filesInDirectory.length; i++) {
            File file = filesInDirectory[i];
            Files files = Files.matches(file.getName());
            pool.execute(new XmlToCsvConverter(file.getAbsolutePath(), files.columns, files.callback));
        }
        pool.shutdown();
        pool.awaitTermination(1000, TimeUnit.MINUTES);
    }

    private static final ExecutorService pool = Executors.newFixedThreadPool(4);


    private static class PostsProcessCallback implements ProcessCallback {

        private CSVWriter postRels, postAnswers, usersPosts,tagsPosts;
        private File directory;

        @Override
        public void start(String fileBaseName) {
            directory = new File(fileBaseName).getParentFile();
            postRels = XmlToCsvConverter.createCsvWriter(path("PostsRels"));
            postAnswers = XmlToCsvConverter.createCsvWriter(path("PostsAnswers"));
            usersPosts = XmlToCsvConverter.createCsvWriter(path("UsersPosts"));
            tagsPosts = XmlToCsvConverter.createCsvWriter(path("TagsPosts"));
        }

        public String path(String name) {
            return new File(directory, name).getAbsolutePath();
        }

        @Override
        public void forRow(int row, XMLStreamReader xmlStreamReader) {
            String id = xmlStreamReader.getAttributeValue(null, "Id");

            String parentId = xmlStreamReader.getAttributeValue("", "ParentId");
            String acceptedAnswerId = xmlStreamReader.getAttributeValue("", "AcceptedAnswerId");
            String ownerUserId = xmlStreamReader.getAttributeValue("", "OwnerUserId");
            String tags = xmlStreamReader.getAttributeValue("", "Tags");

            if (parentId!=null) postRels.writeNext(new String[] {parentId, id});
            if (acceptedAnswerId!=null) postAnswers.writeNext(new String[] {id, acceptedAnswerId});
            if (ownerUserId!=null) usersPosts.writeNext(new String[] {ownerUserId, id});
            if (tags!=null) for (String tag : tags.replace("<", "").split(">")) tagsPosts.writeNext(new String[]{id, tag});
        }

        @Override
        public void end() {
            try {
                postRels.close();
                postAnswers.close();
                usersPosts.close();
                tagsPosts.close();

                XmlToCsvConverter.writeHeader(path("PostsRels"), ":END_ID(Post)", ":START_ID(Post)");
                XmlToCsvConverter.writeHeader(path("PostsAnswers"), ":START_ID(Post)", ":END_ID(Post)");
                XmlToCsvConverter.writeHeader(path("UsersPosts"), ":START_ID(User)", ":END_ID(Post)");
                XmlToCsvConverter.writeHeader(path("TagsPosts"), ":START_ID(Post)", ":END_ID(Tag)");
            } catch (IOException ioe) {
                throw new RuntimeException(ioe);
            }
        }
    }

}
