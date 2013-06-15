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
              'get_clusters_centroid':"select b2.id,'Cluster', ST_AsText(ST_CENTROID(ST_COLLECT(ic.coord))) from vw_inst_coord ic, tb_boundary b2, tb_school s where b2.type=1 and s.bid=b2.id and ic.instid = s.id and b2.id=%s GROUP BY b2.id",
              'get_blocks':"select bcoord.id_bndry,ST_AsText(bcoord.coord),initcap(b.name) from vw_boundary_coord bcoord, tb_boundary b where bcoord.type='Block' and b.id=bcoord.id_bndry order by b.name",
              'get_block_clusters':"select distinct b2.id, b2.name from tb_boundary b, tb_boundary b1, tb_boundary b2,tb_bhierarchy hier where b.id=b1.parent and b1.id=b2.parent  and b.hid = hier.id and b.type=1 and b1.id=%s order by b2.name",
              'get_blocks_centroid':"select b1.id, 'Block', ST_AsText(ST_CENTROID(ST_COLLECT(ic.coord))) from vw_inst_coord ic, tb_boundary b1, tb_boundary b2, tb_school s where b1.type=1 and s.bid = b2.id and s.id =ic.instid and b1.id=b2.parent and b1.id=%s GROUP BY b1.id",
              'get_circles':"select bcoord.id_bndry,ST_AsText(bcoord.coord),initcap(b.name) from vw_boundary_coord bcoord, tb_boundary b where bcoord.type='Circle' and b.id=bcoord.id_bndry order by b.name",
              'get_circles_centroid':"select b2.id,'Circle',ST_AsText(ST_CENTROID(ST_COLLECT(ic.coord))) from vw_inst_coord ic, tb_boundary b2, tb_school s where b2.type=2 and s.bid=b2.id and ic.instid = s.id and b2.id=%s GROUP BY b2.id",
              'get_projects':"select bcoord.id_bndry,ST_AsText(bcoord.coord),initcap(b.name) from vw_boundary_coord bcoord, tb_boundary b where bcoord.type='Project' and b.id=bcoord.id_bndry order by b.name",
              'get_projects_centroid':"select b1.id, 'Project', ST_AsText(ST_CENTROID(ST_COLLECT(ic.coord))) from vw_inst_coord ic, tb_boundary b1, tb_boundary b2, tb_school s where b1.type=2 and s.bid = b2.id and s.id =ic.instid and b1.id=b2.parent and b1.id=%s GROUP BY b1.id"
              }


              # 'get_cluster_centroid':"select instid from vw_inst_coord bcoord, tb_boundary b, tb_boundary b1, tb_boundary b2, tb_school s,tb_bhierarchy hier where b.id=b1.parent and b1.id=b2.parent and s.bid=b2.id and b.hid = hier.id and b.type=1 and b2.id=%s and bcoord.instid=s.id",
              # 'get_block_centroid':"select id_bndry from vw_boundary_coord bcoord, tb_boundary b, tb_boundary b1, tb_boundary b2, tb_bhierarchy hier where b.id=b1.parent and b1.id=b2.parent and b.hid = hier.id and b.type=1 and b1.id=%s and bcoord.id_bndry=b2.id",
              # 'get_block_centroid1':"select id_bndry from vw_boundary_coord bc, tb_boundary b1, tb_boundary b2 where and bc.id_bndry= b2.id and b1.id=b2.parent and b1.id=%s"}

def main():
    settings = configure()

    conn_www = "host='localhost' dbname='"+settings['dbname']+"' user='"+settings['user']+"' password='"+settings['password']+"'"
    conn = psycopg2.connect(conn_www)
    print "Connected!\n"
    cursor = conn.cursor()

    operations = open('operations.sql','w')

    boundaries = ['clusters', 'blocks', 'circles', 'projects']
    counts = {'clusters':{'total':0, 'missed':0}, 'blocks':{'total':0, 'missed':0},
    'circles':{'total':0, 'missed':0},'projects':{'total':0, 'missed':0}}
    for boundary in boundaries:
        print "Fetching", boundary
        cursor.execute(queries['get_'+boundary])
        data = cursor.fetchall()
        counts[boundary]['total']=(data.__len__())
        found = 0

        operations.write("#"+boundary+'\n')
        for d in data:
            id = list(d)[0]
            cursor.execute(queries['get_'+boundary+'_centroid'],(id,))
            centroids = cursor.fetchall()
            if centroids:
                found = found+1
                centroid = list(centroids[0])
                query = "INSERT INTO boundary_coord VALUES ("+str(centroid[0])+", '"+centroid[1]+"', ST_GeomFromText('"+centroid[2]+"',"+"4326))"
                operations.write(query+';\n')
        counts[boundary]['missed']=(data.__len__())-found
    print counts
if __name__ == '__main__':
    main()