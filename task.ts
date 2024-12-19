import { Static, Type, TSchema } from '@sinclair/typebox';
import xml2js from 'xml2js';
import ETL, { Event, SchemaType, handler as internal, local, InputFeatureCollection, InputFeature } from '@tak-ps/etl';
import moment from 'moment-timezone';

export interface Share {
    ShareId: string;
    Password?: string;
}

export default class Task extends ETL {
    static name = 'etl-spot';

    async schema(type: SchemaType = SchemaType.Input): Promise<TSchema> {
        if (type === SchemaType.Input) {
            return Type.Object({
                'SPOT_MAP_SHARES': Type.Array(Type.Object({
                    ShareId: Type.String({
                        description: 'Spot Share ID'
                    }),
                    Password: Type.Optional(Type.String({
                        description: 'Optional Password for the Share Stream'
                    })),
                }), {
                    description: 'Spot Share IDs to pull data from',
                }),
                'DEBUG': Type.Boolean({
                    default: false,
                    description: 'Print results in logs'
                })
            })
        } else {
            return Type.Object({
                messengerName: Type.String({
                    description: 'Human Readable name of the Spot Messenger'
                }),
                dateTime: Type.String({
                    description: 'Time at which the message was recieved',
                    format: 'date-time'
                }),
                messengerId: Type.String({
                    description: 'Device ID of the Spot Messenger'
                }),
                modelId: Type.String({
                    description: 'Model ID of the Spot Messenger'
                }),
                batteryState: Type.String({
                    description: 'Battery level as reported by the device'
                })
            })
        }
    }

    async control(): Promise<void> {
        const layer = await this.fetchLayer();

        if (!layer.environment.SPOT_MAP_SHARES) throw new Error('No SPOT_MAP_SHARES Provided');
        if (!Array.isArray(layer.environment.SPOT_MAP_SHARES)) throw new Error('SPOT_MAP_SHARES must be an array');

        const obtains: Array<Promise<Static<typeof InputFeature>[]>> = [];
        for (const share of layer.environment.SPOT_MAP_SHARES) {
            obtains.push((async (share: Share): Promise<Static<typeof InputFeature>[]> => {
                console.log(`ok - requesting ${share.ShareId}`);

                const url = new URL(`/spot-main-web/consumer/rest-api/2.0/public/feed/${share.ShareId}/latest.xml`, 'https://api.findmespot.com')
                if (share.Password) url.searchParams.append('feedPassword', share.Password);

                const kmlres = await fetch(url);
                const body = await kmlres.text();

                const features: Static<typeof InputFeature>[] = [];

                if (!body.trim()) return features;

                const xml = await xml2js.parseStringPromise(body);

                if (!xml.response) throw new Error('XML Parse Error: Response not found');

                if (xml.response.errors && Array.isArray(xml.response.errors)) {
                    const unknown: Error[] = []
                    for (let err of xml.response.errors) {
                        if (err.error && Array.isArray(err.error)) {
                            err = err.error[0];
                            if (err.code[0] === 'E-0195') {
                                console.log(err.description[0]);
                                xml.response.feedMessageResponse = [];
                            } else {
                                unknown.push(new Error(err.desription[0]));
                            }
                        }
                    }

                    if (unknown.length) {
                        throw new Error(unknown.map((e) => { return e.message }).join(','));
                    }
                }

                if (!xml.response.feedMessageResponse) throw new Error('XML Parse Error: FeedMessageResponse not found');
                if (!xml.response.feedMessageResponse.length) return features;

                console.log(`ok - ${share.ShareId} has ${xml.response.feedMessageResponse[0].count[0]} messages`);
                for (const message of xml.response.feedMessageResponse[0].messages[0].message) {
                    if (moment().diff(moment(message.dateTime[0]), 'minutes') > 30) continue;

                    const feat: Static<typeof InputFeature> = {
                        id: `spot-${message.messengerId[0]}`,
                        type: 'Feature',
                        properties: {
                            callsign: message.messengerName[0],
                            time: new Date(message.dateTime[0]).toISOString(),
                            start: new Date(message.dateTime[0]).toISOString(),
                            metadata: {
                                messengerName: message.messengerName[0],
                                messengerId: message.messengerId[0],
                                modelId: message.modelId[0],
                                batteryState: message.batteryState[0],
                                dateTime: message.dateTime[0],
                            }
                        },
                        geometry: {
                            type: 'Point',
                            coordinates: [ Number(message.longitude[0]), Number(message.latitude[0]), Number(message.altitude[0]) ]
                        }
                    };

                    features.push(feat);
                }

                return features;
            })(share))
        }

        const fc: Static<typeof InputFeatureCollection> = {
            type: 'FeatureCollection',
            features: []
        }

        for (const res of await Promise.all(obtains)) {
            if (!res || !res.length) continue;
            fc.features.push(...res);
        }

        await this.submit(fc);
    }
}

await local(new Task(import.meta.url), import.meta.url);
export async function handler(event: Event = {}) {
    return await internal(new Task(import.meta.url), event);
}
