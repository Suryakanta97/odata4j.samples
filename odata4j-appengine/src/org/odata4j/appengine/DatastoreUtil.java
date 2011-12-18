package org.odata4j.appengine;

import java.util.ArrayList;
import java.util.List;

import com.google.appengine.api.datastore.DatastoreService;
import com.google.appengine.api.datastore.Entity;
import com.google.appengine.api.datastore.Query;

public class DatastoreUtil {

  public static List<String> getKinds(DatastoreService ds) {
    Query q = new Query(Query.KIND_METADATA_KIND);
    List<String> kinds = new ArrayList<String>();
    for (Entity e : ds.prepare(q).asIterable()) {
      kinds.add(e.getKey().getName());
    }
    return kinds;
  }

}
