import pymongo
import sys

WIDTH = 10;
def extractStages(explain):
    # Expecting something like [{$cursor: {winningPlan: {}, executionStats:
    # {..., executionStages: {stage: <NAME>, inputStage: {...}}}}, {$group:
    # {...}, executionStats: {...}}, ...]

    # First flatten that structure into just an array.
    stages = []  # Schema within: {name: String, stats: Object}

    def stageName(aggStage):
        for key in obj:
            if key.startswith("$"):
                return key

    def addPlanStages(stage):
        if "inputStage" in stage:
            addPlanStages(stage["inputStage"])

        stages.append({
            "name": stage["stage"][:WIDTH],
            "stats": {
                "advanced": stage["advanced"],
                "needTime": stage["needTime"],
                "isEOF": stage["isEOF"]
            }
        })

    for obj in explain: 
        if "$cursor" in obj:
            stage = obj["$cursor"]["executionStats"]["executionStages"]
            addPlanStages(stage)

        stageStats = {}
        if "executionStats" in obj:
            stageStats = obj["executionStats"]
        else:
            stageStats = obj[stageName(obj)]["executionStats"]
        if "executionStages" in stageStats:
            stageStats = stageStats["executionStages"]
        stages.append({
            "name": stageName(obj)[:WIDTH],
            "stats": {
                "advanced": stageStats["advanced"],
                "needTime": stageStats["needTime"],
                "isEOF": stageStats["isEOF"],
                "time": stageStats["executionTimeMillisEstimate"]
            }
        })

    return stages

def runAggProgressMeter(db, collName, pipeline):
    res = db.command({
        "aggregate": collName,
        "pipeline": pipeline,
        "cursor": {},
        "slowQueryMS": 30,
        "allowDiskUse": True,
    })

    stages = extractStages(res["stages"]) if "stages" in res else []
    for stage in stages:
        sys.stdout.write((("%" + str(WIDTH) + "s") % stage["name"]) + " ")
    sys.stdout.write("\n")

    for stage in stages:
        sys.stdout.write(" " * (WIDTH + 1))
    sys.stdout.write("\r")

    results = res["cursor"]["firstBatch"]
    while res["cursor"]["id"] != 0:
        res = db.command({
            "getMore": res["cursor"]["id"],
            "collection": collName,
            "slowQueryMS": 250,
        })
        results.extend(res["cursor"]["nextBatch"])
        stages = extractStages(res["cursor"]["stages"] if "stages" in res["cursor"] else [])
        for stage in stages:
            if stage["stats"]["isEOF"] == True:
                sys.stdout.write("x" * WIDTH + " ")
            elif (stage["stats"]["advanced"] > 0 or stage["stats"]["needTime"] > 0):
                sys.stdout.write("." * WIDTH + " ")
            else:
                sys.stdout.write(" " * (WIDTH + 1))

        if len(stages) > 0:
            sys.stdout.write(" %dms" % stages[-1]["stats"]["time"])
        sys.stdout.write("\r")
        sys.stdout.flush()
    sys.stdout.write("\n")
    for result in results:
        print(result)
    print("Done!")


if __name__ == "__main__":
    client = pymongo.MongoClient("10.1.21.131:27017");
    print("Running pipeline...")
    runAggProgressMeter(client.github, "commits", [{"$match": {"committer.type": {"$ne": "User"},
        "committer.id": {"$gte": 400000}}},
        {"$unwind":"$committer"}, {"$match": {"files": {"$size": 1}}}, {"$sort":{"_id":-1}},
        {"$sort":{"_id":-1}},{"$match": {}}, {"$group":{"_id":"$committer.id", "count": {"$sum":1}}},
        {"$sort":{"_id":-1}},{"$match": {}}, {"$sort":{"_id":-1}},{"$limit": 2}])
