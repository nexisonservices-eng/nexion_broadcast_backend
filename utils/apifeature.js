// utils/apiFeatures.js
class APIFeatures {
    constructor(query, queryString) {
        this.query = query;
        this.queryString = queryString;
        this.filterConditions = {};
    }

    // Filtering
    filter() {
        const queryObj = { ...this.queryString };
        const excludedFields = ['page', 'sort', 'limit', 'fields', 'search'];
        excludedFields.forEach(el => delete queryObj[el]);

        // Advanced filtering
        let queryStr = JSON.stringify(queryObj);
        queryStr = queryStr.replace(/\b(gte|gt|lte|lt)\b/g, match => `$${match}`);

        this.filterConditions = JSON.parse(queryStr);
        this.query = this.query.find(this.filterConditions);

        return this;
    }

    // Sorting
    sort() {
        if (this.queryString.sort) {
            const sortBy = this.queryString.sort.split(',').join(' ');
            this.query = this.query.sort(sortBy);
        } else {
            this.query = this.query.sort('-createdAt');
        }

        return this;
    }

    // Field limiting
    limitFields() {
        if (this.queryString.fields) {
            const fields = this.queryString.fields.split(',').join(' ');
            this.query = this.query.select(fields);
        } else {
            this.query = this.query.select('-__v');
        }

        return this;
    }

    // Pagination
    paginate() {
        const page = parseInt(this.queryString.page) || 1;
        const limit = parseInt(this.queryString.limit) || 10;
        const skip = (page - 1) * limit;

        this.query = this.query.skip(skip).limit(limit);

        return this;
    }

    // Search
    search(fields = []) {
        if (this.queryString.search && fields.length > 0) {
            const searchRegex = new RegExp(this.queryString.search, 'i');
            const searchConditions = fields.map(field => ({
                [field]: searchRegex
            }));
            
            this.query = this.query.find({ $or: searchConditions });
            
            // Update filter conditions for count
            this.filterConditions = {
                ...this.filterConditions,
                $or: searchConditions
            };
        }

        return this;
    }
}

module.exports = APIFeatures;