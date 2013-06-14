import psycopg2
import json

def configure():
    with open('settings.json') as f:
        json_data = f.read()
        settings = json.loads(json_data)
        return settings
    return None

queries = {'get_clusters':"select bcoord.id_bndry, ST_AsText(bcoord.coord),initcap(b.name) from vw_boundary_coord bcoord, tb_boundary b where bcoord.type='Cluster' and b.id=bcoord.id_bndry order by b.name",
              'get_schools':"select distinct s.id, s.name from tb_boundary b, tb_boundary b1, tb_boundary b2,tb_school s,tb_bhierarchy hier where b.id=b1.parent and b1.id=b2.parent and s.bid=b2.id and b.hid = hier.id and b.type=1 and b2.id=%s order by s.name",
              'get_cluster_centroid':"select ST_AsText(ST_CENTROID(ST_COLLECT(coord))) from vw_inst_coord, tb_boundary b, tb_boundary b1, tb_boundary b2, tb_school s,tb_bhierarchy hier where b.id=b1.parent and b1.id=b2.parent and s.bid=b2.id and b.hid = hier.id and b.type=1 and b2.id=%s",
              'get_blocks':"select bcoord.id_bndry,ST_AsText(bcoord.coord),initcap(b.name) from vw_boundary_coord bcoord, tb_boundary b where bcoord.type='Block' and b.id=bcoord.id_bndry order by b.name",
              'get_block_clusters':"select distinct b2.id, b2.name from tb_boundary b, tb_boundary b1, tb_boundary b2,tb_bhierarchy hier where b.id=b1.parent and b1.id=b2.parent  and b.hid = hier.id and b.type=1 and b1.id=%s order by b2.name",
              'get_block_centroid':"select ST_CENTROID(ST_COLLECT(coord)) from vw_boundary_coord, tb_boundary b, tb_boundary b1, tb_boundary b2,tb_bhierarchy hier where b.id=b1.parent and b1.id=b2.parent  and b.hid = hier.id and b.type=1 and b1.id=%s"}

def main():
    settings = configure()
    #Define our connection string
    conn_www = "host='localhost' dbname='"+settings['dbname']+"' user='"+settings['user']+"' password='"+settings['password']+"'"
    conn_coord = "host='localhost' dbname='klp-coord' user='"+settings['user']+"' password='"+settings['password']+"'"
    # print the connection string we will use to connect
    # print conn_string
 
    # get a connection, if a connect cannot be made an exception will be raised here
    conn = psycopg2.connect(conn_www)
    print "Connected!\n"
 
    # conn.cursor will return a cursor object, you can use this cursor to perform queries
    cursor = conn.cursor()

    
    # records = cursor.fetchall()
    # for row in records:
    #   print row

    #Get all the clusters
    cursor.execute(queries['get_clusters'])
    clusters = cursor.fetchall()

    #For a cluster, find all the schools.
    #cursor.execute(statements['get_'+type+'_points'],(id,))

    # clusters = clusters[:10]

    # for cluster in clusters:
    #     id = list(cluster)[0]
    #     print id
    #     # cursor.execute(queries['get_schools'],(id,))
    #     # schools = cursor.fetchall()
    #     cursor.execute(queries['get_cluster_centroid'],(id,))
    #     centroid = cursor.fetchall()
    #     print centroid

    #Get all the blocks
    cursor.execute(queries['get_blocks'])
    blocks = cursor.fetchall()

    #For a block, find all the clusters.
    # blocks = blocks[:4]
    # for block in blocks:
    #     id = list(block)[0]
    #     print id
    #     cursor.execute(queries['get_block_centroid'],(id,))
    #     centroid = cursor.fetchall()
    #     print centroid

            

if __name__ == '__main__':
    main()