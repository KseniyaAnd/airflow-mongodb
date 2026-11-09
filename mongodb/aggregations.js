db.comments.aggregate([
    {
        $group: {
            _id: "$content",
            count: { $sum: 1 }
        }
    },
    {
        $sort: { count: -1 }
    },
    {
        $limit: 5
    }
])

db.comments.aggregate([
    {
        $match: {
            $expr: {
                $lt: [
                    { $strLenCP: "$content" },
                    5
                ]
            }
        }
    }
])

db.comments.aggregate([
    {
        $addFields: {
            date: {
                $dateFromString: {
                    dateString: "$created_date",
                    format: "%Y-%m-%d %H:%M:%S"
                }
            }
        }
    },
    {
        $group: {
            _id: {
                $dateTrunc: {
                    date: "$date",
                    unit: "day"
                }
            },
            average_rating: {
                $avg: "$rating"
            }
        }
    },
    {
        $sort: {
            _id: 1
        }
    }
])