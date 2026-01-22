import axios from 'axios';
import { Asset, DescribedImage } from './dataLayer.mjs';
import logger from '../log.mjs';
import sharp from 'sharp';
const log = logger(import.meta.url);

const DEFAULT_IMAGE_WIDTH = 700;

export async function createAsset(asset) {
    log.debug("createAsset pageId=%s, name=%s, size=%s, mimeType=%s", asset.pageId, asset.name, asset.size, asset.mimeType);
    // if (asset.pageId) {
    //     asset.pageId = [asset.pageId];
    // }
    // return Asset.create(asset);
    const createObj = Object.assign({}, asset);
    if (! createObj.uniqueName) createObj.uniqueName = asset.name;
    createObj.image = asset.data;
    return DescribedImage.create(createObj);
}

export function createAssetReference(assetOrId) {
    return '/asset/via/' + ((typeof assetOrId === 'object') ? assetOrId._id.toString() : assetOrId)
}
export async function captureImage(imageUrl, pageId) {
    try {
        log.debug("captureImage imageUrl=%s, pageId=%s", imageUrl, pageId);
        const headers =  {         
            Authorization: undefined  // This removes the header for this request
        }
        const isBase64 = imageUrl.startsWith('data:');
        if (isBase64) {
            const matches = imageUrl.match(/^data:(.+);base64,(.*)$/);
            const buf = Buffer.from(matches[2], 'base64');
            const resize = await sharp(buf).resize(DEFAULT_IMAGE_WIDTH).toBuffer();
            const asset = {
                pageId,
                name: imageUrl,
                size: resize.length,
                data: resize,
                mimeType:matches[1] 
            }
            return createAsset(asset);
            
        } else {
            const response = await axios.get(imageUrl, {responseType: 'arraybuffer', headers});
            if (response.data) {
                const buf = Buffer.from(response.data, 'binary');
                const resize = await sharp(buf).resize(DEFAULT_IMAGE_WIDTH).toBuffer();
                const asset = {
                    pageId,
                    name: imageUrl,
                    size: resize.length,
                    data: resize,
                    mimeType:  response.headers['content-type']
                }
                return createAsset(asset);
            }
        }
        return null;
    } catch (err) {
        log.warn("captureImage error=%o, imageUrl=%s, pageId=%s", err, imageUrl, pageId);
    }
}
